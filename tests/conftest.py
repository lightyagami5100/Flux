import os
import sys
from unittest.mock import MagicMock

# Pin the credentials the suite runs against BEFORE app.config is imported.
# These are assignments, not setdefault: a developer's real ROBOFLOW_API_KEY in
# .env must never leak into a test run and bill the live account. Environment
# variables outrank the .env file in pydantic-settings, so this wins.
os.environ["ENVIRONMENT"] = "test"
os.environ["ROBOFLOW_API_KEY"] = "test-roboflow-key"
os.environ["ROBOFLOW_MODEL_IDS"] = "test-model/1"

# Mock cv2 and inference_sdk if they are not installed
try:
    import cv2  # type: ignore
except ImportError:
    mock_cv2 = MagicMock()
    mock_cv2.VideoCapture = MagicMock()
    sys.modules['cv2'] = mock_cv2

try:
    import inference_sdk  # type: ignore
except ImportError:
    mock_inference_sdk = MagicMock()
    mock_inference_sdk.InferenceHTTPClient = MagicMock()
    sys.modules['inference_sdk'] = mock_inference_sdk
