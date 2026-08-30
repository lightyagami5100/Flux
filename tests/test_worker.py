"""Worker delivery-semantics tests.

The worker is the component that actually spends money on the external inference
API and writes the rows the map renders, so the contract under test is:
  - a successful message is persisted and then ACKed (never the other way round)
  - unreachable media is NOT fabricated into a blank frame
  - permanently broken input goes straight to the DLQ instead of burning retries
  - transient failures stay UNACKED until the retry budget is exhausted
"""

import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config import Settings
from app.processors.base import PermanentProcessingError
from app.schemas import MediaRef


@pytest.fixture
def anyio_backend() -> str:
    """anyio ships the pytest plugin FastAPI already depends on; no extra dev dep."""
    return "asyncio"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        environment="test",
        ingest_stream="stream:test",
        ingest_consumer_group="test-workers",
        minio_bucket="media",
    )


@pytest.fixture
def worker(settings: Settings):
    """A DetectionWorker with MinIO and the processor replaced by mocks."""
    from app.worker import DetectionWorker

    with patch("app.worker.MediaStore") as store_cls, patch("app.worker.create_processor") as make:
        store_cls.return_value = MagicMock()
        make.return_value = MagicMock()
        instance = DetectionWorker(AsyncMock(), MagicMock(), settings)

    instance.redis.incr = AsyncMock(return_value=1)
    instance.redis.expire = AsyncMock()
    instance.redis.delete = AsyncMock()
    instance.redis.xack = AsyncMock()
    instance.redis.xadd = AsyncMock()
    return instance


def _envelope(event_id: str | None = None) -> dict[str, str]:
    payload = {
        "event_id": event_id or str(uuid.uuid4()),
        "captured_at": "2026-08-29T12:00:00Z",
        "latitude": 33.72,
        "longitude": 73.09,
        "media": {"kind": "image", "uri": "minio://media/frame.jpg"},
        "objects": [],
    }
    return {
        "data": json.dumps(
            {
                "payload": payload,
                "device_id": "cam-01",
                "received_at": datetime.now(UTC).isoformat(),
            }
        )
    }


class TestMediaFetch:
    @pytest.mark.anyio
    async def test_unreachable_media_never_reaches_the_inference_api(self, worker):
        """A failed download must raise, not synthesise a blank frame.

        Fabricating input would bill Roboflow for a black image and then persist
        the empty result as a successful "no potholes here" reading.
        """
        from app.worker import MediaUnavailable

        worker.store.download = MagicMock(side_effect=OSError("connection refused"))

        with pytest.raises(MediaUnavailable):
            await worker.process_media(MediaRef(kind="image", uri="minio://media/gone.jpg"))

        worker.processor.infer.assert_not_called()

    @pytest.mark.anyio
    async def test_object_key_is_stripped_of_the_bucket_prefix(self, worker):
        worker.store.download = MagicMock(return_value=b"jpegbytes")
        worker.processor.infer.return_value = MagicMock(
            detections=(),
            processor_name="roboflow",
            processor_version="1.0.0",
            model_name="pothole/1",
            metadata={},
        )

        await worker.process_media(MediaRef(kind="image", uri="minio://media/a/b.jpg"))

        worker.store.download.assert_called_once_with("a/b.jpg")


class TestHandleDelivery:
    @pytest.mark.anyio
    async def test_success_persists_before_ack(self, worker):
        order: list[str] = []
        worker._persist_success = AsyncMock(side_effect=lambda *a, **k: order.append("persist"))
        worker._ack = AsyncMock(side_effect=lambda *a: order.append("ack"))
        worker.process_media = AsyncMock(return_value=({"frames_decoded": 1}, []))

        await worker._handle("1-0", _envelope())

        assert order == ["persist", "ack"], "ACK must follow the DB commit, not precede it"
        worker.redis.xadd.assert_not_called()

    @pytest.mark.anyio
    async def test_malformed_message_goes_straight_to_dlq(self, worker):
        await worker._handle("1-0", {"data": "not json"})

        worker.redis.xadd.assert_awaited_once()
        assert worker.redis.xadd.await_args.args[0] == "stream:test:dlq"
        worker.redis.xack.assert_awaited_once()

    @pytest.mark.anyio
    async def test_message_without_data_field_is_discarded(self, worker):
        await worker._handle("1-0", {})

        worker.redis.xack.assert_awaited_once()
        worker.redis.xadd.assert_not_called()

    @pytest.mark.anyio
    async def test_permanent_error_skips_the_retry_budget(self, worker):
        """Undecodable media must DLQ on attempt 1, not attempt 5."""
        worker.process_media = AsyncMock(side_effect=PermanentProcessingError("not a video"))
        worker._persist_failure = AsyncMock()

        await worker._handle("1-0", _envelope())

        worker._persist_failure.assert_awaited_once()
        worker.redis.xadd.assert_awaited_once()
        worker.redis.xack.assert_awaited_once()

    @pytest.mark.anyio
    async def test_transient_error_stays_unacked_for_redelivery(self, worker):
        worker.process_media = AsyncMock(side_effect=TimeoutError("upstream slow"))
        worker._persist_failure = AsyncMock()

        await worker._handle("1-0", _envelope())

        worker.redis.xack.assert_not_called()
        worker.redis.xadd.assert_not_called()
        worker._persist_failure.assert_not_called()

    @pytest.mark.anyio
    async def test_transient_error_dlqs_once_attempts_are_exhausted(self, worker):
        from app.worker import MAX_ATTEMPTS

        worker.redis.incr = AsyncMock(return_value=MAX_ATTEMPTS)
        worker.process_media = AsyncMock(side_effect=TimeoutError("upstream slow"))
        worker._persist_failure = AsyncMock()

        await worker._handle("1-0", _envelope())

        worker._persist_failure.assert_awaited_once()
        worker.redis.xadd.assert_awaited_once()
        worker.redis.xack.assert_awaited_once()
