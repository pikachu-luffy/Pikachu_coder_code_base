import time
import logging
import pendulum
from datetime import timedelta, datetime
import pandas as pd
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from google.cloud import bigquery
from airflow.models import Variable


JOBCONFIG: dict = Variable.get("in_ya_unsub_rep_config", deserialize_json=True)
# ── CONFIG ────────────────────────────────────────────────────────────

CONFIG = {
    'project_id': JOBCONFIG['project_id'],
    'dataset': JOBCONFIG['dataset'],
    'table_id': JOBCONFIG['table_id'],
    'bucket_name': JOBCONFIG['bucket_name'],
    'gcs_prefix': JOBCONFIG['gcs_prefix'],
    'hardcoded_last_gen': JOBCONFIG['hardcoded_last_gen'],  # -1 for full load, or a positive int for incremental
    'poke_interval_seconds': JOBCONFIG['poke_interval_seconds'],  # 1h
    'sensor_timeout_seconds': JOBCONFIG['sensor_timeout_seconds'],  # 2h
}


POKE_INTERVAL_SECONDS = 3600
SENSOR_TIMEOUT_SECONDS = 60 * 60 * 2

DEFAULT_ARGS = {
    "owner": "mdlz_in_ya_cde",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger("unsubscribe_dag")
logger.setLevel(logging.INFO)

# ── UTILITY FUNCTIONS ──────────────────────────────────────────────────
def log_task_failure(context):
    exception = context.get('exception')
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    logging.error(
        f"[DAG Failure] DAG ID: '{dag_id}', Task ID: '{task_id}', Execution Date: '{execution_date}'"
    )
    logging.error(
        f"[DAG Failure] Exception occurred: {type(exception).__name__}: {exception}"
    )


def decide_next_step(**context):
    try:
        if CONFIG['hardcoded_last_gen'] < 0:
            logger.info("First run detected. Skipping sensor, proceeding to direct file processing.")
            return "process_files_direct"
        else:
            logger.info("Incremental run detected. Using sensor to wait for new files.")
            return "wait_for_new_files"
    except Exception as e:
        logger.error("Error in branching decision: %s", str(e))
        raise

def list_candidate_files(gcs, last_gen):
    blobs = gcs.list(bucket_name=CONFIG['bucket_name'], prefix=CONFIG['gcs_prefix'])
    candidates = []

    for blob_name in blobs:
        if not blob_name.lower().endswith('.csv'):
            continue

        try:
            metadata = gcs.get_blob_metadata(bucket_name=CONFIG['bucket_name'], blob_name=blob_name)
            gen = int(metadata.get('generation', 0))
            if gen > last_gen:
                candidates.append({'path': blob_name, 'generation': gen})
                logger.info("Candidate file: %s (generation: %s)", blob_name, gen)
        except Exception as e:
            logger.warning("Could not get metadata for %s: %s", blob_name, str(e))
            if last_gen < 0:
                candidates.append({'path': blob_name, 'generation': 0})

    return candidates

def process_candidates(candidates, gcs, bq_client):
    for item in candidates:
        blob_path = item['path']
        uri = f"gs://{CONFIG['bucket_name']}/{blob_path}"
        logger.info("Downloading: %s", uri)

        with tempfile.NamedTemporaryFile("wb", suffix=".csv") as tmp:
            gcs.download(bucket_name=CONFIG['bucket_name'], object_name=blob_path, filename=tmp.name)
            df = pd.read_csv(tmp.name)

        for col in ['phone', 'user_request', 'created_at', 'updated_at']:
            if col in df.columns:
                df[col] = df[col].astype(str)

        ist = pendulum.timezone("Asia/Kolkata")
        df['ingestion_timestamp'] = datetime.now(ist)

        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        logger.info("Uploading %d rows to BigQuery: %s", len(df), CONFIG['table_id'])
        try:
            job = bq_client.load_table_from_dataframe(df, CONFIG['table_id'], job_config=job_config)
            job.result()
            logger.info("Upload successful for %s", blob_path)
        except Exception as e:
            logger.error("Upload failed for %s: %s", blob_path, str(e))
            raise

def process_files_fn(**context):
    try:
        logger.info("Starting file processing.")
        gcs = GCSHook()
        bq_client = bigquery.Client(project=CONFIG['project_id'])

        last_gen = HARDCODED_LAST_GEN
        mode = "Full load" if last_gen < 0 else "Incremental"
        logger.info("%s mode (last_gen=%s)", mode, last_gen)

        candidates = list_candidate_files(gcs, last_gen)
        if not candidates:
            logger.info("No files to process.")
            return

        logger.info("%d files to process.", len(candidates))
        process_candidates(candidates, gcs, bq_client)

    except Exception as e:
        logger.error("Error in process_files_fn: %s", str(e))
        raise

# ── DAG DEFINITION ─────────────────────────────────────────────────────
with DAG(
    dag_id="in_ya_unsubscribe_report_load",
    default_args=DEFAULT_ARGS,
    description="Incremental GCS→BQ load with first-run branch and sensor (hardcoded watermark)",
    schedule_interval="0 14 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["gcs", "bigquery", "production", "hardcoded"],
) as dag:

    decide = BranchPythonOperator(
        task_id="decide_next_step",
        python_callable=decide_next_step,
        trigger_rule="all_done",
        on_failure_callback=log_task_failure,
    )

    wait_for_file = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_files",
        bucket=CONFIG['bucket_name'],
        prefix=CONFIG['gcs_prefix'],
        poke_interval=CONFIG['poke_interval_seconds'],
        timeout=CONFIG['sensor_timeout_seconds'],
        mode="poke",
        on_failure_callback=log_task_failure,
    )

    process_files_direct = PythonOperator(
        task_id="process_files_direct",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    process_files_incremental = PythonOperator(
        task_id="process_files_incremental",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    decide >> [wait_for_file, process_files_direct]
    wait_for_file >> process_files_incremental
