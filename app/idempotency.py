"""Redis-backed idempotency for the ingest endpoint.

Flow per (device, key):
  1. reserve() -> atomically claims the key with SET NX (marker "pending:<token>").
                  - key holds a completed response -> returned as a Replay.
                  - key holds a pending marker     -> caller answers 409 Conflict.
  2. store()   -> replaces the marker with the final response (TTL restarts:
                  a sliding 24h replay window from completion).
  3. release() -> deletes the marker if producing failed, so the client can
                  safely retry with the same key.

A crash between reserve() and store() leaves a pending marker that expires with
the TTL; clients get 409 until then — the safe outcome (never silently
double-enqueue).
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis

_PENDING_PREFIX = "pending:"


@dataclass(frozen=True, slots=True)
class Replay:
    status_code: int
    body: dict


async def reserve(client: Redis, key: str, ttl_seconds: int) -> tuple[bool, Replay | None]:
    """Try to claim `key`. Returns (claimed, replay)."""
    token = uuid.uuid4().hex

    if await client.set(key, _PENDING_PREFIX + token, nx=True, ex=ttl_seconds):
        return True, None

    raw = await client.get(key)
    if raw is None:
        # Expired between SET NX and GET — one retry.
        if await client.set(key, _PENDING_PREFIX + token, nx=True, ex=ttl_seconds):
            return True, None
        raw = await client.get(key)

    if raw is None:
        return False, None  # pathological race; caller returns 409, client retries

    if raw.startswith(_PENDING_PREFIX):
        return False, None  # another request is in flight

    try:
        stored = json.loads(raw)
        return False, Replay(status_code=int(stored["status_code"]), body=stored["body"])
    except Exception:
        # Corrupted entry: drop it and let this request claim a fresh key.
        await client.delete(key)
        if await client.set(key, _PENDING_PREFIX + token, nx=True, ex=ttl_seconds):
            return True, None
        return False, None


async def store(client: Redis, key: str, status_code: int, body: dict, ttl_seconds: int) -> None:
    await client.set(key, json.dumps({"status_code": status_code, "body": body}), ex=ttl_seconds)


async def release(client: Redis, key: str) -> None:
    await client.delete(key)
