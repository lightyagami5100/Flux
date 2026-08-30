"""Video frame-sampling contract for the Roboflow processor.

The video path used to send a synthetic black 640x640 frame to the inference API
and report the result as a successful video analysis, which meant every clip the
mobile app uploaded produced a guaranteed zero detections. These tests pin the
real behaviour: sample every Nth frame, cap the number of billed calls, and fail
loudly on media that will not decode.
"""

from unittest.mock import MagicMock, patch

import cv2
import numpy as np
import pytest

from app.processors import MediaType, create_processor
from app.processors.base import PermanentProcessingError
from app.processors.roboflow import MediaUndecodable


def _encode_clip(tmp_path, frame_count: int, fps: int = 30) -> bytes:
    """Write a real mp4 with a known frame count and return its bytes."""
    path = tmp_path / "clip.mp4"
    writer = cv2.VideoWriter(
        str(path), cv2.VideoWriter_fourcc(*"mp4v"), fps, (64, 64)
    )
    if not writer.isOpened():
        pytest.skip("no mp4v encoder available in this OpenCV build")
    for i in range(frame_count):
        frame = np.full((64, 64, 3), i % 256, dtype=np.uint8)
        writer.write(frame)
    writer.release()
    return path.read_bytes()


@pytest.fixture
def processor():
    with patch("app.processors.roboflow.InferenceHTTPClient") as client_cls:
        client = MagicMock()
        client.infer.return_value = {"predictions": []}
        client_cls.return_value = client
        instance = create_processor("roboflow")
        instance.load()
        yield instance


class TestVideoSampling:
    def test_samples_every_nth_frame(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=30)
        processor.settings = processor.settings.model_copy(
            update={"video_sample_every_n_frames": 10, "video_max_frames": 20}
        )

        frames = processor._sample_video_frames(media, "clip.mp4")

        assert [index for index, _ts, _img in frames] == [0, 10, 20]

    def test_respects_the_billed_call_ceiling(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=30)
        processor.settings = processor.settings.model_copy(
            update={"video_sample_every_n_frames": 1, "video_max_frames": 4}
        )

        frames = processor._sample_video_frames(media, "clip.mp4")

        assert len(frames) == 4, "video_max_frames must bound the number of API calls"

    def test_timestamps_are_derived_from_fps(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=20, fps=10)
        processor.settings = processor.settings.model_copy(
            update={"video_sample_every_n_frames": 10, "video_max_frames": 20}
        )

        frames = processor._sample_video_frames(media, "clip.mp4")

        # frame 10 at 10fps is one second in
        assert [ts for _index, ts, _img in frames] == [0, 1000]

    def test_infer_calls_the_api_once_per_sampled_frame(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=30)
        processor.settings = processor.settings.model_copy(
            update={"roboflow_model_ids": ["potholes/7"], "video_sample_every_n_frames": 10, "video_max_frames": 20}
        )

        result = processor.infer(media, MediaType.VIDEO, "clip.mp4")

        assert processor.client.infer.call_count == 3
        assert result.metadata["frames_inferred"] == 3
        assert result.media_type == MediaType.VIDEO

    def test_multiple_models_each_run_on_every_frame_and_merge(self, processor, tmp_path):
        """N models = N billed calls per frame, detections merged and labeled per model."""
        media = _encode_clip(tmp_path, frame_count=20, fps=10)
        processor.settings = processor.settings.model_copy(
            update={
                "roboflow_model_ids": ["potholes/7", "damage-road/1"],
                "video_sample_every_n_frames": 10,
                "video_max_frames": 20,
            }
        )

        def fake_infer(image, model_id=None):
            cls = "pothole" if model_id == "potholes/7" else "crack"
            return {"predictions": [{"class": cls, "confidence": 0.7, "x": 32, "y": 32, "width": 8, "height": 8}]}

        processor.client.infer.side_effect = fake_infer

        result = processor.infer(media, MediaType.VIDEO, "clip.mp4")

        # 2 frames x 2 models
        assert processor.client.infer.call_count == 4
        assert sorted(call.kwargs["model_id"] for call in processor.client.infer.call_args_list) == [
            "damage-road/1", "damage-road/1", "potholes/7", "potholes/7",
        ]
        assert sorted(d.class_name for d in result.detections) == ["crack", "crack", "pothole", "pothole"]
        assert result.model_name == "potholes/7,damage-road/1"
        assert result.metadata["detections_per_model"] == {"potholes/7": 2, "damage-road/1": 2}

    def test_detections_carry_frame_provenance(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=20, fps=10)
        processor.settings = processor.settings.model_copy(
            update={"roboflow_model_ids": ["potholes/7"], "video_sample_every_n_frames": 10, "video_max_frames": 20}
        )
        processor.client.infer.return_value = {
            "predictions": [
                {"class": "pothole", "confidence": 0.8, "x": 32, "y": 32, "width": 10, "height": 10}
            ]
        }

        result = processor.infer(media, MediaType.VIDEO, "clip.mp4")

        assert [d.frame_index for d in result.detections] == [0, 10]
        assert [d.timestamp_ms for d in result.detections] == [0, 1000]

    def test_uses_the_configured_model_id(self, processor, tmp_path):
        media = _encode_clip(tmp_path, frame_count=1)
        processor.settings = processor.settings.model_copy(
            update={"roboflow_model_ids": ["potholes/7"], "video_max_frames": 1}
        )

        result = processor.infer(media, MediaType.VIDEO, "clip.mp4")

        assert processor.client.infer.call_args.kwargs["model_id"] == "potholes/7"
        assert result.model_name == "potholes/7"


class TestUndecodableMedia:
    def test_garbage_video_raises_a_permanent_error(self, processor):
        with pytest.raises(MediaUndecodable):
            processor.infer(b"not a video at all", MediaType.VIDEO, "junk.mp4")

        processor.client.infer.assert_not_called()

    def test_garbage_image_raises_a_permanent_error(self, processor):
        with pytest.raises(MediaUndecodable):
            processor.infer(b"not an image", MediaType.IMAGE, "junk.jpg")

        processor.client.infer.assert_not_called()

    def test_undecodable_is_routed_as_permanent(self):
        """The worker keys its no-retry path off this base class."""
        assert issubclass(MediaUndecodable, PermanentProcessingError)
