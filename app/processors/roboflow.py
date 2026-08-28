from __future__ import annotations

import logging
import time
from typing import Any
from tenacity import retry, stop_after_attempt, wait_exponential

class CircuitBreakerOpen(Exception):
    pass

from inference_sdk import InferenceHTTPClient

from app.config import get_settings
from .base import (
    BaseProcessor,
    Detection,
    MediaType,
    ProcessingResult,
)
from app.processors import register_processor

logger = logging.getLogger(__name__)

@register_processor
class RoboflowProcessor(BaseProcessor):
    name = "roboflow"
    
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.settings = get_settings()
        self.client = None
        self.model_id = "yolov8n-640" # Fallback/default if we don't have a specific custom model

    def load(self) -> None:
        """Initialize the InferenceHTTPClient."""
        logger.info("Initializing Roboflow inference client...")
        api_key = self.settings.roboflow_api_key
        if not api_key:
            logger.warning("ROBOFLOW_API_KEY is not set. Inference will fail.")
        
        self.client = InferenceHTTPClient(
            api_url="https://detect.roboflow.com",
            api_key=api_key
        )

    def infer(self, media_bytes: bytes, media_type: MediaType, file_id: str) -> ProcessingResult:
        """Run inference using Roboflow Inference API."""
        if self.client is None:
            self.load()
            
        import numpy as np
        import cv2

        if media_type == MediaType.VIDEO:
            # We would sample frames here, but for simplicity we will just log a warning
            # and try to infer on a blank frame or first frame
            logger.warning("Video inference on Roboflow not fully implemented; falling back to blank image")
            image = np.zeros((640, 640, 3), dtype=np.uint8)
        else:
            # Decode the image bytes
            np_arr = np.frombuffer(media_bytes, np.uint8)
            image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            if image is None:
                raise ValueError(f"Could not decode image bytes for {file_id}")
        # Circuit breaker logic
        if hasattr(self, "_cb_failures") and self._cb_failures >= 3:
            if time.time() < self._cb_reset_time:
                raise CircuitBreakerOpen("Circuit breaker is OPEN. Roboflow API is temporarily unavailable.")
            else:
                self._cb_failures = 0 # Half-open

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True
        )
        def _do_infer():
            return self.client.infer(image, model_id="coco/3")

        detections = []
        try:
            result = _do_infer()
            self._cb_failures = 0 # Success resets circuit breaker
            
            for pred in result.get("predictions", []):
                # Roboflow returns x, y (center), width, height
                x = pred["x"]
                y = pred["y"]
                w = pred["width"]
                h = pred["height"]
                
                # Convert to x1, y1, x2, y2
                x1 = x - (w / 2)
                y1 = y - (h / 2)
                x2 = x + (w / 2)
                y2 = y + (h / 2)
                
                detections.append(Detection(
                    class_id=pred.get("class_id", 0),
                    class_name=pred["class"],
                    confidence=pred["confidence"],
                    bbox=(x1, y1, x2, y2)
                ))
        except Exception as e:
            logger.error(f"Roboflow inference failed: {e}")
            if not hasattr(self, "_cb_failures"):
                self._cb_failures = 0
            self._cb_failures += 1
            if self._cb_failures >= 3:
                self._cb_reset_time = time.time() + 60 # Cooldown for 60 seconds
                logger.error("Circuit breaker OPENED for 60 seconds.")
            raise
            
        return ProcessingResult(
            processor_name=self.name,
            processor_version="1.0.0",
            model_name="coco/3",
            media_type=media_type,
            detections=tuple(detections),
            metadata={"source": "roboflow"}
        )
