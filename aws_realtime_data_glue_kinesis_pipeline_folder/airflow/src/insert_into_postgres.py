import psycopg2
import pandas as pd
import boto3
import logging
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def s3_connection():
    logger.info("Connecting to S3...")
    s3 = boto3.client("s3", region_name="eu-north-1")
    logger.info("Connected to S3 successfully.")
    return s3

def getting_and_processing_data(s3):
    logger.info("Downloading data from S3...")
    obj = s3.get_object(Bucket='landed-flight-data-bucket84h674', Key='data.parquet')
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    logger.info("Data downloaded. Removing duplicates by flight_id...")
    df = df.drop_duplicates(subset=['flight_id'])
    logger.info(f"Dataframe ready with {len(df)} rows.")
    return df

def postgres_connection():
    logger.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="flight-postgres.crkgcg2ayz0w.eu-north-1.rds.amazonaws.com", 
        database="postgres",
        user="postgres",      
        password="Cimbom1995."
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL successfully.")
    return cur, conn

def inserting_data_to_postgres(cur, conn, df):
    logger.info("Inserting data into PostgreSQL...")
    for i, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO flights (
                flight_id, airline, flight_status,
                departure_city, departure_airport,
                arrival_city, arrival_airport,
                scheduled_departure_time, actual_departure_time,
                scheduled_arrival_time, actual_arrival_time,
                arrival_delay_min, departure_delay_min,
                delayed_status, remaining_distance_km,
                completion_pct, flight_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['flight_id'], row['airline'], row['flight_status'],
                row['departure_city'], row['departure_airport'],
                row['arrival_city'], row['arrival_airport'],
                row['scheduled_departure_time'], row['actual_departure_time'],
                row['scheduled_arrival_time'], row['actual_arrival_time'],
                row['arrival_delay_min'], row['departure_delay_min'],
                row['delayed_status'], row['remaining_distance_km'],
                row['completion_pct'], row['flight_date']
            )
        )
    conn.commit()
    logger.info("Data inserted and committed successfully.")

def getting_and_inserting_data():
    try:
        s3 = s3_connection()
        df = getting_and_processing_data(s3)
        cur, conn = postgres_connection()
        inserting_data_to_postgres(cur, conn, df)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        try:
            cur.close()
            conn.close()
            logger.info("PostgreSQL connection closed.")
        except:
            pass


