import time
import logging
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from google.cloud import bigquery
from airflow.operators.empty import EmptyOperator

# ── CONFIG ────────────────────────────────────────────────────────────
PROJECT_ID      = "dev-amea-cde-data-prj"
DATASET         = "in_raw_hubspot"
TABLE_ID        = f"{PROJECT_ID}.{DATASET}.in_ya_unsubscribe_report"
BUCKET_NAME     = "in-cde-yellowai-file-upload-dev-bkt"
GCS_PREFIX      = "x1631881181412/bot_tables/unsubscribe_requests"

# HARDCODED watermark for testing (set to -1 for full load, or a positive int for incremental)
HARDCODED_LAST_GEN = -1  # Change this value as needed

# Sensor configuration
POKE_INTERVAL_SECONDS = 3600       # 1h
SENSOR_TIMEOUT_SECONDS = 60 * 60 * 2  # 2h

DEFAULT_ARGS = {
    "owner": "mdlz_in_ya_cde",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def decide_next_step(**context):
    """
    Branch logic: if first run (watermark < 0), skip sensor and go to processing;
    otherwise go to sensor.
    """
    last_gen = HARDCODED_LAST_GEN
    if last_gen < 0:
        logger.info("First run: skipping sensor, going straight to process_files_direct")
        return "process_files_direct"
    else:
        logger.info("Incremental run: invoking sensor")
        return "wait_for_new_files"


def process_files_fn(**context):
    """
    Read watermark, list new CSVs, update watermark, load to BigQuery.
    """
    gcs = GCSHook()
    print(f"GCS Hook initialized : {gcs}")
    bq = bigquery.Client(project=PROJECT_ID)
    print(f"BigQuery Client initialized : {bq}")

    # Fetch watermark
    last_gen = HARDCODED_LAST_GEN
    if last_gen < 0:
        logger.info("Full load mode (last_gen=%s)", last_gen)
    else:
        logger.info("Incremental mode (last_gen=%s)", last_gen)

    # List CSVs
    blobs = gcs.list(bucket_name=BUCKET_NAME, prefix=GCS_PREFIX)
    candidates = []
    for name in blobs:
        if not name.lower().endswith('.csv'):
            continue
        meta = gcs.get_blob(bucket_name=BUCKET_NAME, blob_name=name)
        gen = int(meta.generation)
        if gen > last_gen:
            candidates.append({'path': name, 'generation': gen})

    if not candidates:
        logger.info("No new files to process (last_gen=%s)", last_gen)
        return

    # Update watermark (for logging only, since it's hardcoded)
    max_gen = max(item['generation'] for item in candidates)
    logger.info("Watermark would be updated to %s (hardcoded mode, not persisted)", max_gen)

    # Load to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("phone",             "STRING"),
            bigquery.SchemaField("user_request",      "STRING"),
            bigquery.SchemaField("created_at",        "TIMESTAMP"),
            bigquery.SchemaField("updated_at",        "TIMESTAMP"),
            bigquery.SchemaField(
                "ingestion_timestamp", "TIMESTAMP",
                mode="NULLABLE",
                default_value_expression="CURRENT_TIMESTAMP()",
            ),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    for item in candidates:
        uri = f"gs://{BUCKET_NAME}/{item['path']}"
        logger.info("Loading %s into %s", uri, TABLE_ID)
        start = time.time()
        job = bq.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
        job.result()
        elapsed = time.time() - start
        logger.info("Loaded %s in %.2f seconds", item['path'], elapsed)


# ── DAG Definition ────────────────────────────────────────────────────────
with DAG(
    dag_id="in_ya_unsubscribe_report_load",
    default_args=DEFAULT_ARGS,
    description="Incremental GCS→BQ load with first-run branch and sensor (hardcoded watermark)",
    schedule_interval="0 14 * * *",  # 14:00 UTC == 20:00 IST
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["gcs", "bigquery", "production", "hardcoded"],
) as dag:

    # Branch to skip sensor on first run
    decide = BranchPythonOperator(
        task_id="decide_next_step",
        python_callable=decide_next_step,
        trigger_rule="all_done",
        on_failure_callback=lambda context: logger.error(
            f"[DAG Failure] Task {context['task_instance'].task_id} failed: {context['exception']}"
        ),
    )

    # Sensor for incremental loads
    wait_for_file = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_files",
        bucket=BUCKET_NAME,
        prefix=GCS_PREFIX,
        poke_interval=POKE_INTERVAL_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="poke",
    )

    # Direct processing for first run (full load)
    process_files_direct = PythonOperator(
        task_id="process_files_direct",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
    )

    # Processing after sensor for incremental loads
    process_files_incremental = PythonOperator(
        task_id="process_files_incremental",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
    )

    # Define the flow
    decide >> [wait_for_file, process_files_direct]
    wait_for_file >> process_files_incremental