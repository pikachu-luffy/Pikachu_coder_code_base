from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka configuration"""

    bootstrap_servers: str = "localhost:29092"
    frame_topic: str = "frame_topic"


kafka_config = KafkaConfig()
