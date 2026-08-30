"""Abstract contract for pluggable perception processors."""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, ClassVar

logger = logging.getLogger(__name__)


class PermanentProcessingError(Exception):
    """The input can never succeed on retry, e.g. media that will not decode.

    Callers should route these straight to the dead-letter queue instead of
    spending the retry budget on them.
    """


class MediaType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"

    @classmethod
    def from_value(cls, value: str) -> MediaType:
        try:
            return cls(value.strip().lower())
        except ValueError as exc:
            raise ValueError(f"Unsupported media type: {value!r}") from exc


_VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".mpg", ".mpeg", ".wmv"}
)


def guess_media_type(filename: str) -> MediaType:
    """Fallback when the ingest plane did not set an explicit media_type."""
    ext = os.path.splitext(filename)[1].lower()
    return MediaType.VIDEO if ext in _VIDEO_EXTENSIONS else MediaType.IMAGE


@dataclass(frozen=True, slots=True)
class Detection:
    """A single object detection in pixel coordinates (origin: top-left)."""

    class_id: int
    class_name: str
    confidence: float
    bbox: tuple[float, float, float, float]  # (x1, y1, x2, y2) in original pixels
    frame_index: int | None = None           # populated for video inputs only
    timestamp_ms: int | None = None          # populated for video inputs only


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    processor_name: str
    processor_version: str
    model_name: str
    media_type: MediaType
    detections: tuple[Detection, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable representation"""
        return {
            "processor": self.processor_name,
            "processor_version": self.processor_version,
            "model": self.model_name,
            "media_type": self.media_type.value,
            "detection_count": len(self.detections),
            "detections": [asdict(d) for d in self.detections],
            "metadata": self.metadata,
        }


class BaseProcessor(ABC):
    """Contract every perception backend must fulfil."""

    #: Unique registry key, e.g. "yolov8". Must be set by subclasses.
    name: ClassVar[str]
    #: Semantic version of the processor implementation.
    version: ClassVar[str] = "0.0.0"

    @abstractmethod
    def load(self) -> None:
        """One-time heavy init: download/load weights, warm up the model."""

    @abstractmethod
    def infer(
        self,
        media_bytes: bytes,
        media_type: MediaType,
        filename: str = "",
    ) -> ProcessingResult:
        """Run inference on raw media bytes and return structured detections."""

    def health_check(self) -> bool:
        """Optional liveness probe for the loaded model."""
        return True
