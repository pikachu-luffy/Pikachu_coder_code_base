import boto3
import logging
import time
import json
from datetime import datetime

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ----------------------------
# Configurations
# ----------------------------
RAW_DATA_BUCKET = "raw-data-flight-bucket84h674"  # S3 bucket for storing raw data
STREAM_NAME = "flight_map_kinesis_stream"         # Kinesis stream name
SHARD_ID = "shardId-000000000000"                # Shard to read from
POLL_INTERVAL = 1  # seconds between polls
RECORD_LIMIT = 10  # number of records to fetch per poll

# ----------------------------
# Connect to Kinesis
# ----------------------------
def kinesis_connection():
    logger.info("Connecting to Kinesis...")
    try:
        kinesis = boto3.client("kinesis", region_name="eu-north-1")
        logger.info("Connected to Kinesis successfully.")
        return kinesis
    except Exception as e:
        logger.error(f"Failed to connect to Kinesis: {e}")
        raise

# ----------------------------
# Connect to S3
# ----------------------------
def s3_connection():
    logger.info("Connecting to S3...")
    try:
        s3 = boto3.client("s3", region_name="eu-north-1")
        logger.info("Connected to S3 successfully.")
        return s3
    except Exception as e:
        logger.error(f"Failed to connect to S3: {e}")
        raise

# ----------------------------
# Get shard iterator for reading
# ----------------------------
def get_shard_iterator(kinesis, stream_name, shard_id):
    logger.info(f"Getting shard iterator for stream {stream_name}, shard {shard_id}...")
    try:
        response = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'  # start from oldest record
        )
        return response['ShardIterator']
    except Exception as e:
        logger.error(f"Failed to get shard iterator: {e}")
        raise

# ----------------------------
# Process Kinesis records and save to S3
# ----------------------------
def process_records(kinesis, s3, shard_iterator):
    while True:
        try:
            # Fetch records from Kinesis
            out = kinesis.get_records(ShardIterator=shard_iterator, Limit=RECORD_LIMIT)
            records = out.get('Records', [])

            if records:
                batch = []
                ts = records[0]['ApproximateArrivalTimestamp']  # timestamp of first record
                folder = ts.strftime("year=%Y/month=%m/day=%d/hour=%H")  # folder structure

                # Decode and parse JSON data
                for record in records:
                    data = record['Data'].decode('utf-8')
                    batch.append(json.loads(data))

                # Save batch to S3
                first_seq = records[0]['SequenceNumber']
                last_seq = records[-1]['SequenceNumber']
                s3_key = f"kinesis/{folder}/batch_{first_seq}_{last_seq}.json"

                try:
                    s3.put_object(Bucket=RAW_DATA_BUCKET, Key=s3_key, Body=json.dumps(batch))
                    logger.info(f"Batch saved to S3: {s3_key} ({len(batch)} records)")
                except Exception as e:
                    logger.error(f"Failed to save batch to S3: {e}")

            # Move to next shard iterator
            shard_iterator = out.get('NextShardIterator')
            if not shard_iterator:
                logger.warning("No next shard iterator, exiting loop.")
                break

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"Error while reading from Kinesis: {e}")
            time.sleep(POLL_INTERVAL)

# ----------------------------
# Main execution
# ----------------------------
def main():
    kinesis = kinesis_connection()
    s3 = s3_connection()
    shard_iterator = get_shard_iterator(kinesis, STREAM_NAME, SHARD_ID)

    logger.info("Starting Kinesis -> S3 streaming...")
    process_records(kinesis, s3, shard_iterator)

if __name__ == "__main__":
    main()
