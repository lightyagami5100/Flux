import sys
from unittest.mock import MagicMock

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
