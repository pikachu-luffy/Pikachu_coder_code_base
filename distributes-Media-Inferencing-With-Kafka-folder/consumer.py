import json
import os
import threading
from config import kafka_config
from confluent_kafka import Consumer
from loguru import logger

from inference import infer_media_frame
from storage import annotated_frames_path, raw_frames_path


class MediaConsumer:
    """Handles the consumption of media frames from the Kafka topic"""

    def __init__(self):
        self.kafka_config = kafka_config

    def consume_media_frames(self) -> bool:
        """Consume media frames from the Kafka topic"""
        consumer = Consumer(
            {
                "bootstrap.servers": self.kafka_config.bootstrap_servers,
                "group.id": "media-consumer",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([self.kafka_config.frame_topic])
        logger.info(
            f"Subscribed to {self.kafka_config.frame_topic} | Thread {threading.current_thread().name}"
        )
        try:
            while True:
                message = consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    logger.error(f"Error consuming media frames: {message.error()}")
                    continue
                consumer.commit(message)
                frame_bytes = message.value().decode("utf-8")
                frame = json.loads(frame_bytes)
                frame_path = os.path.join(
                    raw_frames_path, frame["video_name"], f"{frame['frame_no']}.jpg"
                )

                output_path = os.path.join(
                    annotated_frames_path,
                    frame["video_name"],
                    f"{frame['frame_no']}.jpg",
                )
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                infer_media_frame(frame_path, output_path)
                logger.info(f"Annotated frame saved to {output_path}")
                consumer.commit(message, asynchronous=True)
        except Exception as e:
            logger.error(f"Error consuming media frames: {e}")
            return False
        finally:
            consumer.close()
            logger.info("Closed consumer")
            return True

    def start(self, num_threads: int = 10):
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=self.consume_media_frames)
            t.daemon = True
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
