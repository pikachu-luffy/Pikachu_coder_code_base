#!/usr/bin/env python3
import pathlib
import click
from loguru import logger
from confluent_kafka.admin import AdminClient, NewTopic
from config import kafka_config
from consumer import MediaConsumer
from procucer import MediaProducer


@click.group()
def cli():
    """CLI tool for distributed media inferencing"""


@cli.command()
def setup():
    """Setup the project"""
    logger.info("Setting up the project")
    logger.info("Setting up kafka topics...")

    try:
        admin = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
        admin.create_topics([NewTopic(kafka_config.frame_topic, 3, 1)])
        logger.info(f"Kafka | {kafka_config.frame_topic} created successfully")
    except Exception as e:
        logger.error(f"Error setting up kafka topics: {e}")
        raise e


@cli.command()
def produce():
    """Produce media frames to the Kafka topic"""
    logger.info("Producing media frames to the Kafka topic")

    media_paths = [
        video_path
        for video_path in pathlib.Path("media/videos_to_process").glob("*.mp4")
    ]
    media_producer = MediaProducer()
    media_producer.start(media_paths)


@cli.command()
def consume():
    """Consume media frames from the Kafka topic"""
    logger.info("Consuming media frames from the Kafka topic")
    media_consumer = MediaConsumer()
    media_consumer.start(num_threads=3)


if __name__ == "__main__":
    cli()
