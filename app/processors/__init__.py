from __future__ import annotations

import importlib
import logging
from typing import Any, Type

from .base import (
    BaseProcessor,
    Detection,
    MediaType,
    ProcessingResult,
    guess_media_type,
)

logger = logging.getLogger(__name__)

__all__ = [
    "BaseProcessor",
    "Detection",
    "MediaType",
    "ProcessingResult",
    "guess_media_type",
    "register_processor",
    "create_processor",
    "available_processors",
]

_REGISTRY: dict[str, Type[BaseProcessor]] = {}

# Built-ins are imported lazily so heavy ML dependencies (torch, ultralytics)
# are only loaded in processes that actually use them.
_BUILTINS: dict[str, str] = {
    "roboflow": "app.processors.roboflow",
}


def register_processor(cls: Type[BaseProcessor]) -> Type[BaseProcessor]:
    """Class decorator: add a BaseProcessor subclass to the registry."""
    if not (isinstance(cls, type) and issubclass(cls, BaseProcessor)):
        raise TypeError("@register_processor expects a BaseProcessor subclass")
    key = getattr(cls, "name", "").strip().lower()
    if not key:
        raise ValueError(f"{cls.__name__} must define a non-empty `name`")
    if key in _REGISTRY:
        raise ValueError(f"Processor name collision on {key!r}")
    _REGISTRY[key] = cls
    logger.debug("Registered perception processor: %s", key)
    return cls


def available_processors() -> list[str]:
    return sorted(set(_REGISTRY) | set(_BUILTINS))


def create_processor(name: str, **kwargs: Any) -> BaseProcessor:
    """Instantiate a processor by registry name (lazy-imports built-ins)."""
    key = name.strip().lower()
    if key not in _REGISTRY and key in _BUILTINS:
        importlib.import_module(_BUILTINS[key])  # triggers @register_processor
    cls = _REGISTRY.get(key)
    if cls is None:
        raise KeyError(f"Unknown processor {name!r}. Available: {available_processors()}")
    return cls(**kwargs)
