import json
import logging
import requests
import boto3
import time

# Configure logging to display timestamp, log level, and message
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# API endpoint for fetching flight data
url = "http://127.0.0.1:5000/api/flights"

def fetch_data(url):
    """Fetch data from the API and return as a JSON string."""
    try:
        response = requests.get(url, timeout=10)  # Send GET request with 10s timeout
        if response.status_code != 200:
            logger.error(f"API error {response.status_code}")  # Log if API response is not OK
            return
        data = response.json()  # Parse JSON response
        data_string = json.dumps(data)  # Convert JSON to string for Kinesis
        return data_string
    except Exception as e:
        logger.error(f"Unexpected error {e}")  # Log any unexpected exceptions
        return

def kinesis_connection():
    """Create and return a Kinesis client."""
    kinesis_client = boto3.client("kinesis", region_name="eu-north-1")
    return kinesis_client

def send_to_kinesis(data_string, kinesis_client):
    """Send the data string to the specified Kinesis stream."""
    logger.info("Data is being sent to Kinesis")
    try:
        kinesis_client.put_record(
            StreamName="flight_map_kinesis_stream",  # Name of the Kinesis stream, we will need it once we read the data
            Data=data_string.encode("utf-8"),       # Encode string to bytes
            PartitionKey="flight-1"                # Partition key for routing in stream
        )
    except Exception as e:
        logger.error(f"An error has occurred {e}")  # Log any exceptions during sending

if __name__ == "__main__":
    kinesis_client = kinesis_connection()
    
    while True:
        data_string = fetch_data(url)  # Fetch latest flight data from API
        if not data_string:
            logger.warning("No data fetched. Skipping Kinesis send.")  # Skip if no data
        else:
            send_to_kinesis(data_string, kinesis_client)  # Send data to Kinesis
        
        time.sleep(2)  # Wait 2 seconds before fetching again
