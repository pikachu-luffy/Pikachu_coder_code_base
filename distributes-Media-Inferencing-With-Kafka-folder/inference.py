import threading
import cv2
from ultralytics import YOLO
from loguru import logger

_thread_local = threading.local()


def get_model():
    """Get the model for the current thread"""
    if not hasattr(_thread_local, "model"):
        _thread_local.model = YOLO("yolo11n.pt")
    return _thread_local.model


def infer_media_frame(frame_path: str, output_path: str) -> bool:
    """Infer a media frame - thread safe"""
    try:
        model = get_model()
        results = model.predict(frame_path, conf=0.25, iou=0.7)

        annotated_frame = results[0].plot()

        cv2.imwrite(output_path, annotated_frame)
        logger.info(f"Annotated frame saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error inferring media frame: {e}")
        return False
