from __future__ import annotations

import logging
import tempfile
import time
from typing import Any

try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:  # pragma: no cover - tenacity is a declared dependency
    def retry(stop=None, wait=None, reraise=True):
        def decorator(func):
            def wrapper(*args, **kwargs):
                attempts = 3
                for attempt in range(attempts):
                    try:
                        return func(*args, **kwargs)
                    except Exception:
                        if attempt == attempts - 1:
                            if reraise:
                                raise
                            return None
                        time.sleep(0.05 * (2 ** attempt))
            return wrapper
        return decorator

    def stop_after_attempt(n):
        return n

    def wait_exponential(**kwargs):
        return kwargs

try:
    from inference_sdk import InferenceHTTPClient
except ImportError:
    InferenceHTTPClient = None  # type: ignore[assignment]

try:
    import numpy as np
except ImportError:  # pragma: no cover - numpy is a declared dependency
    np = None  # type: ignore[assignment]

from app.config import get_settings
from app.processors import register_processor

from .base import (
    BaseProcessor,
    Detection,
    MediaType,
    PermanentProcessingError,
    ProcessingResult,
)

logger = logging.getLogger(__name__)


class CircuitBreakerOpen(Exception):
    """Raised while the breaker is open and calls are being short-circuited."""


class ProcessorUnavailable(Exception):
    """Raised when the external inference backend cannot be constructed at all."""


class MediaUndecodable(PermanentProcessingError):
    """Raised when the supplied bytes are not a decodable image or video."""


@register_processor
class RoboflowProcessor(BaseProcessor):
    name = "roboflow"
    version = "1.0.0"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.settings = get_settings()
        self.client: Any = None
        # Fallback/default if we don't have a specific custom model.
        self.model_id = "yolov8n-640"
        self._cb_failures: int = 0
        self._cb_reset_time: float = 0.0

    def load(self) -> None:
        """Initialize the InferenceHTTPClient."""
        logger.info("Initializing Roboflow inference client...")
        if InferenceHTTPClient is None:
            raise ProcessorUnavailable(
                "inference-sdk is not installed; the Roboflow processor cannot run. "
                "Install it (see requirements.txt) or set PROCESSOR_NAME to another backend."
            )

        api_key = self.settings.roboflow_api_key
        if not api_key:
            raise ProcessorUnavailable(
                "ROBOFLOW_API_KEY is not set; refusing to start the Roboflow processor."
            )

        self.client = InferenceHTTPClient(
            api_url="https://detect.roboflow.com",
            api_key=api_key,
        )

    def infer(self, media_bytes: bytes, media_type: MediaType, file_id: str) -> ProcessingResult:
        """Run inference using the Roboflow Inference HTTP API."""
        if self.client is None:
            self.load()

        if media_type == MediaType.VIDEO:
            frames = self._sample_video_frames(media_bytes, file_id)
        else:
            frames = [(0, 0, self._decode_image(media_bytes, file_id))]

        model_ids = self.settings.roboflow_model_ids or ["coco/3"]
        detections: list[Detection] = []
        per_model_counts: dict[str, int] = {}
        for frame_index, timestamp_ms, image in frames:
            for model_id in model_ids:
                found = self._infer_frame(image, file_id, frame_index, timestamp_ms, model_id)
                per_model_counts[model_id] = per_model_counts.get(model_id, 0) + len(found)
                detections.extend(found)

        return ProcessingResult(
            processor_name=self.name,
            processor_version=self.version,
            model_name=",".join(model_ids),
            media_type=media_type,
            detections=tuple(detections),
            metadata={"frames_inferred": len(frames), "detections_per_model": per_model_counts},
        )

    # ------------------------------------------------------------- decoding
    def _decode_image(self, media_bytes: bytes, file_id: str) -> Any:
        """Decode raw image bytes into a BGR array."""
        import cv2

        if np is None:
            raise ProcessorUnavailable("numpy is required to decode media bytes.")

        image = cv2.imdecode(np.frombuffer(media_bytes, np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise MediaUndecodable(f"Could not decode image bytes for {file_id}")
        return image

    def _sample_video_frames(
        self, media_bytes: bytes, file_id: str
    ) -> list[tuple[int, int, Any]]:
        """Decode a clip and return every Nth frame as (index, timestamp_ms, image).

        Inference is billed per call, so the clip is subsampled and capped. cv2 has
        no bytes-based reader, hence the temp file.
        """
        import cv2

        step = max(1, self.settings.video_sample_every_n_frames)
        limit = max(1, self.settings.video_max_frames)

        with tempfile.NamedTemporaryFile(suffix=".mp4") as handle:
            handle.write(media_bytes)
            handle.flush()

            capture = cv2.VideoCapture(handle.name)
            if not capture.isOpened():
                capture.release()
                raise MediaUndecodable(f"Could not open video bytes for {file_id}")

            fps = capture.get(cv2.CAP_PROP_FPS) or 0.0
            frames: list[tuple[int, int, Any]] = []
            frame_index = 0
            try:
                while len(frames) < limit:
                    ok, frame = capture.read()
                    if not ok:
                        break
                    if frame_index % step == 0:
                        timestamp_ms = int(frame_index / fps * 1000) if fps > 0 else 0
                        frames.append((frame_index, timestamp_ms, frame))
                    frame_index += 1
            finally:
                capture.release()

        if not frames:
            raise MediaUndecodable(f"Video {file_id} yielded no decodable frames")

        logger.info(
            "sampled %d frame(s) from %s (every %d, cap %d)", len(frames), file_id, step, limit
        )
        return frames

    # ------------------------------------------------------------ inference
    def _infer_frame(
        self, image: Any, file_id: str, frame_index: int, timestamp_ms: int, model_id: str
    ) -> list[Detection]:
        """One external API call against one model, guarded by the circuit breaker."""
        if self._cb_failures >= 3:
            if time.time() < self._cb_reset_time:
                raise CircuitBreakerOpen("Circuit breaker is OPEN. Roboflow API is temporarily unavailable.")
            self._cb_failures = 0  # half-open: allow one probe through

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True,
        )
        def _do_infer() -> dict[str, Any]:
            return self.client.infer(image, model_id=model_id)

        detections: list[Detection] = []
        try:
            result = _do_infer()
            self._cb_failures = 0  # success resets the breaker

            for pred in result.get("predictions", []):
                # Roboflow returns center-x, center-y, width, height.
                x = pred["x"]
                y = pred["y"]
                w = pred["width"]
                h = pred["height"]

                detections.append(Detection(
                    class_id=pred.get("class_id", 0),
                    class_name=pred["class"],
                    confidence=pred["confidence"],
                    bbox=(x - (w / 2), y - (h / 2), x + (w / 2), y + (h / 2)),
                    frame_index=frame_index,
                    timestamp_ms=timestamp_ms,
                ))
        except Exception as e:
            logger.error("Roboflow inference failed for %s frame %d (model %s): %s", file_id, frame_index, model_id, e)
            self._cb_failures += 1
            if self._cb_failures >= 3:
                self._cb_reset_time = time.time() + 60
                logger.error("Circuit breaker OPENED for 60 seconds.")
            raise

        return detections
