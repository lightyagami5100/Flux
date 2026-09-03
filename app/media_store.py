from __future__ import annotations

import logging

try:
    from minio import Minio
except ImportError:
    Minio = None  # type: ignore

logger = logging.getLogger(__name__)


class MediaStore:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ) -> None:
        self.bucket = bucket
        self._client = None
        if Minio is not None:
            try:
                self._client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                )
                if not self._client.bucket_exists(self.bucket):
                    self._client.make_bucket(self.bucket)
            except Exception as e:
                logger.warning(f"Could not verify/create bucket {self.bucket}: {e}")

    def download(self, object_key: str) -> bytes:
        import os
        if object_key.startswith("file://"):
            local_path = object_key.replace("file://", "")
            with open(local_path, "rb") as f:
                return f.read()
        if os.path.exists(object_key):
            with open(object_key, "rb") as f:
                return f.read()
        if self._client is not None:
            try:
                response = self._client.get_object(self.bucket, object_key)
                try:
                    return response.read()
                finally:
                    response.close()
                    response.release_conn()
            except Exception:
                pass

        # Check local media directory fallback (data/media/mobile)
        local_candidate = os.path.join("data", "media", "mobile", os.path.basename(object_key))
        if os.path.exists(local_candidate):
            with open(local_candidate, "rb") as f:
                return f.read()
        raise FileNotFoundError(f"Media object could not be retrieved: {object_key}")
