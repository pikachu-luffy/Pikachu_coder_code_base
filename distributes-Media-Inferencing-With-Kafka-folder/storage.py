import os
import pathlib
from loguru import logger

storage_path = os.path.join(os.path.dirname(__file__), "media")
raw_frames_path = os.path.join(storage_path, "raw_frames")
annotated_frames_path = os.path.join(storage_path, "annotated_frames")


def store_frame(frame_bytes: bytes, frame_no: int, video_name: str) -> bool:
    """Store a frame to the storage"""
    try:
        frame_path = os.path.join(raw_frames_path, video_name, f"{frame_no}.jpg")
        os.makedirs(os.path.dirname(frame_path), exist_ok=True)
        with open(frame_path, "wb") as f:
            f.write(frame_bytes)
        logger.info(f"Frame stored successfully: {frame_path}")
        return True
    except Exception as e:
        logger.error(f"Error storing frame: {e}")
        return False
