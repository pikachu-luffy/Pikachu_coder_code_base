from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, StructField, ArrayType, IntegerType
from math import radians, sin, cos, sqrt, atan2
import logging
from datetime import datetime

# Spark & Glue Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KinesisFlightConsumer")
landed_output_path = "s3://landed-flight-data-bucket84h674/"

kinesis_stream_name = "flight_map_kinesis_stream"
region_name = "eu-north-1"
processed_output_path = "s3://processed-data-flight-bucket84h674/"
today_folder = datetime.now().strftime("year=%Y/month=%m/day=%d")
landed_output_path = f"s3://landed-flight-data-bucket84h674/{today_folder}/"


# Function to read from Kinesis
def getting_raw_data():
    logger.info("Initializing raw data stream from Kinesis...")
    raw_df = (
        spark.readStream
            .format("aws-kinesis")
            .option("streamName", kinesis_stream_name)
            .option("region", region_name)
            .option("initialPosition", "TRIM_HORIZON")
            .load()
    )
    logger.info("Successfully created raw data stream DataFrame from Kinesis.")
    return raw_df

# Haversine distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

haversine_udf = udf(haversine, DoubleType())

# Flight schema
def get_flight_schema():
    logger.info("Defining schema for flight data...")
    schema = StructType([
        StructField("flights", ArrayType(
            StructType([
                StructField("actual_departure_time", StringType()),
                StructField("actual_arrival_time", StringType()),
                StructField("airline", StringType()),
                StructField("arrival_airport", StringType()),
                StructField("arrival_city", StringType()),
                StructField("current_altitude_m", DoubleType()),
                StructField("current_location", StructType([
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType())
                ])),
                StructField("current_speed_km_h", DoubleType()),
                StructField("departure_airport", StringType()),
                StructField("departure_city", StringType()),
                StructField("dest_lat", DoubleType()),
                StructField("dest_lon", DoubleType()),
                StructField("direction", DoubleType()),
                StructField("distance_travelled", DoubleType()),
                StructField("distance_travelled_km", DoubleType()),
                StructField("flight_status", StringType()),
                StructField("flight_id", IntegerType()),
                StructField("lat", DoubleType()),
                StructField("lon", DoubleType()),
                StructField("scheduled_arrival_time", StringType()),
                StructField("scheduled_departure_time", StringType()),
                StructField("speed", DoubleType()),
                StructField("start_time", StringType())
            ])
        ))
    ])
    logger.info("Flight schema defined successfully.")
    return schema

# Processing pipeline
def processing_data(raw_df, schema):
    logger.info("Parsing raw data with provided schema...")
    parsed_df = raw_df.withColumn("data", from_json(col("data").cast("string"), schema)) \
                      .select("data.*")

    logger.info("Flattening nested flight array...")
    flattened_df = parsed_df.select(explode(col("flights")).alias("flight")) \
        .select(
            col("flight.actual_departure_time"),
            col("flight.actual_arrival_time"),
            col("flight.airline"),
            col("flight.arrival_airport"),
            col("flight.arrival_city"),
            col("flight.current_altitude_m"),
            col("flight.current_location.latitude").alias("latitude"),
            col("flight.current_location.longitude").alias("longitude"),
            col("flight.current_speed_km_h"),
            col("flight.departure_airport"),
            col("flight.departure_city"),
            col("flight.dest_lat"),
            col("flight.dest_lon"),
            col("flight.direction"),
            col("flight.distance_travelled"),
            col("flight.distance_travelled_km"),
            col("flight.flight_status"),
            col("flight.flight_id"),
            col("flight.lat"),
            col("flight.lon"),
            col("flight.scheduled_arrival_time"),
            col("flight.scheduled_departure_time"),
            col("flight.speed"),
            col("flight.start_time")
        )

    logger.info("Calculating remaining distance using haversine formula...")
    processed_df = flattened_df.withColumn(
        "remaining_distance_km",
        haversine_udf(col("latitude"), col("longitude"), col("dest_lat"), col("dest_lon"))
    )

    logger.info("Calculating completion percentage for flights...")
    processed_df = processed_df.withColumn(
        "completion_pct",
        (col("distance_travelled_km") /
         (col("distance_travelled_km") + col("remaining_distance_km"))) * 100
    )

    logger.info("Data processing completed successfully.")
    return flattened_df, processed_df

# Streaming write
def processed_data_streaming(processed_df, processed_output_path):
    logger.info("Starting writeStream to S3 for processed flight data...")
    processed_stream = processed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", processed_output_path) \
        .option("checkpointLocation", "s3://processed-data-flight-bucket84h674/checkpoint/") \
        .start()

    logger.info("WriteStream started. Awaiting termination...")
    return processed_stream


def processing_landed_flights(flattened_df):
    logger.info("Filtering only landed flights...")

    landed_df = flattened_df.filter(col("flight_status") == 'Landed').select(
        "flight_id",
        "airline",
        "flight_status",
        "departure_city",
        "departure_airport",
        "arrival_city",
        "arrival_airport",
        "scheduled_departure_time",
        "actual_departure_time",
        "scheduled_arrival_time",
        "actual_arrival_time"
    )

    logger.info("Calculating departure delay in minutes...")
    landed_df = landed_df.withColumn(
        "departure_delay_min",
        (unix_timestamp(col("actual_departure_time")) - unix_timestamp(col("scheduled_departure_time"))) / 60
    )

    logger.info("Calculating arrival delay in minutes...")
    landed_df = landed_df.withColumn(
        "arrival_delay_min",
        (unix_timestamp(col("actual_arrival_time")) - unix_timestamp(col("scheduled_arrival_time"))) / 60
    )

    logger.info("Assigning delayed status based on arrival delay...")
    landed_df = landed_df.withColumn(
        "delayed_status",
        when(col("arrival_delay_min") < 15, "on_time")
        .when((col("arrival_delay_min") >= 15) & (col("arrival_delay_min") < 30), "slightly_delayed")
        .otherwise("too_late")
    )

    logger.info("Extracting flight date from actual arrival time...")
    landed_df = landed_df.withColumn(
        "flight_date",
        to_date(col("actual_arrival_time"))
    )

    logger.info("Landed flights DataFrame processed successfully.")
    return landed_df


def landed_flight_streaming(landed_df, landed_output_path):
    logger.info("Starting writeStream for landed flights...")

    landed_stream = landed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", landed_output_path) \
        .option("checkpointLocation", "s3://landed-flight-data-bucket84h674/checkpoint/") \
        .start()

    logger.info("WriteStream started for landed flights. Awaiting termination...")
    return landed_stream


def main():
    logger.info("Starting Glue streaming job...")

    # 1. Read raw data
    raw_df = getting_raw_data()

    # 2. Get schema
    schema = get_flight_schema()

    # 3. Process raw -> flattened + processed
    flattened_df, processed_df = processing_data(raw_df, schema)

    # 4. Process landed flights
    landed_df = processing_landed_flights(flattened_df)

    # 5. Start both streams (processed + landed)
    processed_stream = processed_data_streaming(processed_df, processed_output_path)
    landed_stream = landed_flight_streaming(landed_df, landed_output_path)
        
    logger.info("Both streaming queries started. Awaiting termination...")

if __name__ == "__main__":
    main()
