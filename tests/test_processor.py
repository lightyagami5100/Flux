from unittest.mock import MagicMock, patch
import pytest

from app.processors import create_processor, MediaType, ProcessingResult

def test_processor_creation():
    # Attempting to create an unknown processor raises KeyError
    with pytest.raises(KeyError):
        create_processor("unknown")
    
@patch("app.processors.roboflow.InferenceHTTPClient")
def test_roboflow_processor(mock_client_class):
    # Mock the client instance
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    
    # Create the processor
    processor = create_processor("roboflow")
    
    # Test load
    processor.load()
    assert processor.client is not None

    # Test infer
    import sys
    with patch.dict('sys.modules', {'cv2': MagicMock()}):
        import cv2
        mock_img = MagicMock()
        cv2.imdecode.return_value = mock_img
        
        # Mocking the inference result from Roboflow
        mock_result = {
            "predictions": [
                {
                    "class": "pothole",
                    "confidence": 0.9,
                    "x": 50,
                    "y": 50,
                    "width": 100,
                    "height": 100,
                    "tracker_id": None
                }
            ]
        }
        mock_client_instance.infer.return_value = mock_result
        
        res = processor.infer(b"fake_image_bytes", MediaType.IMAGE, "test.jpg")
        
        assert isinstance(res, ProcessingResult)
        assert len(res.detections) == 1
        assert res.detections[0].class_name == "pothole"
        assert res.detections[0].confidence == 0.9

@patch("app.processors.roboflow.InferenceHTTPClient")
def test_roboflow_circuit_breaker(mock_client_class):
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    processor = create_processor("roboflow")
    processor.load()
    
    import sys
    with patch.dict('sys.modules', {'cv2': MagicMock()}):
        import cv2
        cv2.imdecode.return_value = MagicMock()
        
        # Make the client raise an exception
        mock_client_instance.infer.side_effect = Exception("API down")
        
        from app.processors.roboflow import CircuitBreakerOpen
        import time
        
        # Mock time.sleep to bypass tenacity's exponential backoff wait in tests
        with patch('time.sleep', return_value=None):
            for _ in range(3):
                with pytest.raises(Exception, match="API down"):
                    processor.infer(b"fake", MediaType.IMAGE, "test.jpg")
                
            # Fourth call should raise CircuitBreakerOpen without hitting the API
            mock_client_instance.infer.reset_mock()
            with pytest.raises(CircuitBreakerOpen):
                processor.infer(b"fake", MediaType.IMAGE, "test.jpg")
            
            # Ensure the client wasn't called because circuit breaker is open
            mock_client_instance.infer.assert_not_called()
