import sys
from unittest.mock import MagicMock

sys.modules['cv2'] = MagicMock()
mock_inf = MagicMock()
mock_inf.InferenceHTTPClient = MagicMock()
sys.modules['inference_sdk'] = mock_inf

try:
    import app.processors.roboflow
    print("Import SUCCESS!")
except Exception as e:
    import traceback
    traceback.print_exc()
