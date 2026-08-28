"""FastAPI dependencies: device API-key authentication and Redis access."""
from __future__ import annotations

import secrets
from dataclasses import dataclass

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from redis.asyncio import Redis

from .config import get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass(frozen=True, slots=True)
class DeviceIdentity:
    device_id: str


async def get_current_device(
    request: Request,
    x_api_key: str | None = Security(_api_key_header),
) -> DeviceIdentity:
    """Resolve the caller's device identity from its API key.

    The device_id is ALWAYS derived server-side from the key — never trusted
    from the request body. Comparison is constant-time.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    for candidate, device_id in get_settings().api_keys.items():
        if secrets.compare_digest(candidate.encode(), x_api_key.encode()):
            return DeviceIdentity(device_id=device_id)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
    )


def get_redis(request: Request) -> Redis:
    """Connection pool is owned by the app lifespan (see main.py)."""
    return request.app.state.redis
