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
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        
        # Ensure bucket exists
        try:
            if not self._client.bucket_exists(self.bucket):
                self._client.make_bucket(self.bucket)
        except Exception as e:
            logger.warning(f"Could not verify/create bucket {self.bucket}: {e}")

    def download(self, object_key: str) -> bytes:
        response = self._client.get_object(self.bucket, object_key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
