"""Chunked upload manager for MS-006.

Manages upload sessions, stores individual chunks in MinIO, reassembles
them into a final video file, and cleans up stale/orphaned sessions.

Flow:
  1. create_session()  → returns session_id + expected chunk count
  2. store_chunk()     → stores chunk N for a session
  3. complete_session()→ assembles chunks, uploads final file, triggers ingest
  4. cleanup_stale()   → background sweep for abandoned sessions
"""
from __future__ import annotations

import io
import logging
import tempfile
import time
import uuid
from dataclasses import dataclass, field

try:
    from minio import Minio
except ImportError:
    Minio = None  # type: ignore

logger = logging.getLogger("upload_manager")

# Sessions older than this (seconds) are considered stale and eligible for cleanup
STALE_SESSION_TTL = 24 * 60 * 60  # 24 hours

# Default chunk size hint (5 MB) — informational for the client
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024


@dataclass
class UploadSession:
    """In-memory representation of an active upload session."""
    session_id: str
    device_id: str
    filename: str
    total_chunks: int
    latitude: float
    longitude: float
    content_type: str = "video/mp4"
    received_chunks: set[int] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)


class UploadManager:
    """Manages chunked uploads with MinIO as the backing store."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ) -> None:
        self.bucket = bucket
        if Minio is not None:
            try:
                import urllib3
                http_client = urllib3.PoolManager(
                    timeout=urllib3.Timeout(connect=1.0, read=1.0),
                    retries=urllib3.Retry(total=0),
                )
                self._client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                    http_client=http_client,
                )
            except Exception:
                self._client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                )
        else:
            self._client = None

        # In-memory session registry (swap for Redis in production)
        self._sessions: dict[str, UploadSession] = {}

        # Ensure bucket exists
        if self._client:
            try:
                if not self._client.bucket_exists(self.bucket):
                    self._client.make_bucket(self.bucket)
            except Exception as e:
                logger.warning(f"Could not verify/create bucket {self.bucket}: {e}")

    # ─── Session lifecycle ───────────────────────────────────────────────

    def create_session(
        self,
        device_id: str,
        filename: str,
        total_chunks: int,
        latitude: float,
        longitude: float,
        content_type: str = "video/mp4",
    ) -> UploadSession:
        """Create a new upload session with sanitized inputs and return its metadata."""
        import os
        import re

        # Security hardening: sanitize filename to prevent path traversal
        clean_filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", os.path.basename(filename)) or "uploaded_media.mp4"

        # Boundary checks
        if not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
            raise ValueError(f"Invalid GPS coordinates: lat={latitude}, lon={longitude}")
        if total_chunks <= 0 or total_chunks > 10000:
            raise ValueError(f"Invalid total_chunks: {total_chunks} (must be between 1 and 10000)")

        session_id = uuid.uuid4().hex
        session = UploadSession(
            session_id=session_id,
            device_id=device_id,
            filename=clean_filename,
            total_chunks=total_chunks,
            latitude=latitude,
            longitude=longitude,
            content_type=content_type,
        )
        self._sessions[session_id] = session
        logger.info(
            "created upload session %s for device=%s chunks=%d",
            session_id, device_id, total_chunks,
        )
        return session

    def get_session(self, session_id: str) -> UploadSession | None:
        """Look up a session by ID."""
        return self._sessions.get(session_id)

    # ─── Chunk storage ───────────────────────────────────────────────────

    def store_chunk(self, session_id: str, chunk_index: int, data: bytes) -> bool:
        """Store a single chunk in MinIO. Returns True if stored successfully."""
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"Unknown session: {session_id}")
        if chunk_index < 0 or chunk_index >= session.total_chunks:
            raise ValueError(
                f"Chunk index {chunk_index} out of range [0, {session.total_chunks})"
            )

        object_key = self._chunk_key(session_id, chunk_index)
        self._client.put_object(
            self.bucket,
            object_key,
            io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )
        session.received_chunks.add(chunk_index)
        logger.info(
            "stored chunk %d/%d for session %s (%d bytes)",
            chunk_index + 1, session.total_chunks, session_id, len(data),
        )
        return True

    # ─── Assembly ────────────────────────────────────────────────────────

    def complete_session(self, session_id: str) -> tuple[str, str]:
        """Assemble all chunks into a single file in MinIO.

        Returns (minio_uri, object_key) of the assembled file.
        Raises ValueError if chunks are missing.
        """
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"Unknown session: {session_id}")

        expected = set(range(session.total_chunks))
        missing = expected - session.received_chunks
        if missing:
            raise ValueError(
                f"Cannot complete session {session_id}: "
                f"missing chunks {sorted(missing)}"
            )

        # Reassemble by streaming chunks in order into a temporary file
        final_key = f"uploads/{session_id}/{session.filename}"
        with tempfile.NamedTemporaryFile(suffix=f"_{session.filename}") as tmp:
            for i in range(session.total_chunks):
                chunk_key = self._chunk_key(session_id, i)
                response = self._client.get_object(self.bucket, chunk_key)
                try:
                    while True:
                        buf = response.read(64 * 1024)
                        if not buf:
                            break
                        tmp.write(buf)
                finally:
                    response.close()
                    response.release_conn()

            tmp.flush()
            assembled_size = tmp.tell()
            tmp.seek(0)

            # Upload the assembled file
            self._client.put_object(
                self.bucket,
                final_key,
                tmp,
                length=assembled_size,
                content_type=session.content_type,
            )

        # Clean up individual chunk objects
        for i in range(session.total_chunks):
            try:
                self._client.remove_object(self.bucket, self._chunk_key(session_id, i))
            except Exception:
                logger.warning("failed to remove chunk %d for session %s", i, session_id)

        # Remove the session from memory
        del self._sessions[session_id]

        uri = f"minio://{self.bucket}/{final_key}"
        logger.info(
            "assembled session %s → %s (%d bytes from %d chunks)",
            session_id, final_key, assembled_size, session.total_chunks,
        )
        return uri, final_key

    # ─── Cleanup ─────────────────────────────────────────────────────────

    def cleanup_stale(self) -> int:
        """Remove sessions older than STALE_SESSION_TTL. Returns count cleaned."""
        now = time.time()
        stale_ids = [
            sid for sid, s in self._sessions.items()
            if now - s.created_at > STALE_SESSION_TTL
        ]
        for sid in stale_ids:
            session = self._sessions[sid]
            # Clean up any chunks that were uploaded
            for i in session.received_chunks:
                try:
                    self._client.remove_object(
                        self.bucket, self._chunk_key(sid, i)
                    )
                except Exception:
                    pass
            del self._sessions[sid]
            logger.info("cleaned up stale session %s (age=%.0fs)", sid, now - session.created_at)
        return len(stale_ids)

    # ─── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _chunk_key(session_id: str, chunk_index: int) -> str:
        return f"chunks/{session_id}/{chunk_index:05d}"
