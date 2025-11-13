import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import cv2
import numpy as np
from confluent_kafka import Producer
from loguru import logger

from config import kafka_config
from storage import store_frame


def serialize_frame(frame: np.ndarray) -> bytes:
    """Serialize a frame to a byte array"""
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
    if not success:
        raise ValueError("Failed to encode frame")
    return buffer.tobytes()


class MediaProducer:
    """Handles the production of media frames to the Kafka topic"""

    def __init__(
        self,
        max_workers: int = 10,
        frame_skip: int = 3,
        frame_delay: int = 0.1,
    ):
        self.max_workers = max_workers
        self.kafka_config = kafka_config
        self.frame_skip = frame_skip
        self.frame_delay = frame_delay
        self.producer = Producer(
            {
                "bootstrap.servers": self.kafka_config.bootstrap_servers,
            }
        )

    def produce_media_frames(self, media_path: str):
        """Produce media frames to the Kafka topic"""
        cap = cv2.VideoCapture(media_path)
        if not cap.isOpened():
            logger.error(f"Error opening media file: {media_path}")
            return

        video_name = os.path.splitext(os.path.basename(media_path))[0]
        frame_no = 0
        sent_frames = 0

        try:
            while True:
                ok, frame = cap.read()
                if not ok:
                    break

                frame_no += 1
                if frame_no % self.frame_skip != 0:
                    continue

                try:
                    frame_bytes = serialize_frame(frame)
                    if store_frame(frame_bytes, frame_no, video_name):
                        logger.info(f"Frame {frame_no} stored successfully")
                        self.producer.produce(
                            self.kafka_config.frame_topic,
                            json.dumps(
                                {
                                    "video_name": video_name,
                                    "frame_no": frame_no,
                                }
                            ).encode("utf-8"),
                        )
                        sent_frames += 1
                        logger.info(f"Frame {frame_no} sent successfully to Kafka")
                        time.sleep(self.frame_delay)
                    else:
                        logger.error(f"Frame {frame_no} failed to store")
                        raise ValueError(f"Frame {frame_no} failed to store")
                except Exception as e:
                    logger.error(f"Error producing media frame: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error producing media frames: {e}")
            raise e
        finally:
            cap.release()
            logger.info(
                f"{video_name} | Produced {sent_frames} frames out of {frame_no} frames"
            )

    def start(self, media_paths: list[str]):
        """Start the media producer"""
        with ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="media-producer"
        ) as executor:
            futures = [
                executor.submit(self.produce_media_frames, media_path)
                for media_path in media_paths
            ]

            for future in as_completed(futures):
                media = future.result()
        self.producer.flush(10)
        logger.info("Finished producing media frames")
