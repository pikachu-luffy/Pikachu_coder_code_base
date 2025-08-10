import time
import logging
import pendulum
import tempfile
import pandas as pd
from airflow import DAG
from google.cloud import bigquery
from google.cloud import storage
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor


JOBCONFIG: dict = Variable.get("in_ya_unsub_rep_config", deserialize_json=True)


# ── CONFIG ────────────────────────────────────────────────────────────
CONFIG = {
    'project_id': JOBCONFIG['project_id'],
    'dataset': JOBCONFIG['dataset'],
    'table_id': JOBCONFIG['table_id'],
    'bucket_name': JOBCONFIG['bucket_name'],
    'gcs_prefix': JOBCONFIG['gcs_prefix'],
    'poke_interval_seconds': JOBCONFIG['poke_interval_seconds'],
    'sensor_timeout_seconds': JOBCONFIG['sensor_timeout_seconds']
}

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
    bq = bigquery.Client(project=JOBCONFIG['project_id'])
    sql = f"SELECT COUNT(*) AS cnt FROM `{JOBCONFIG['table_id']}`"
    cnt = next(bq.query(sql).result()).cnt
    if cnt == 0:
        logger.info("Target table empty (%s rows). Proceeding full load.", cnt)
        # return "process_files_direct"
        return ["process_files_direct"]
    else:
        logger.info("Target table has %s rows. Proceeding incremental load.", cnt)
        # return "process_files_incremental"
        return ["wait_for_new_files", "process_files_incremental"]


def list_gcs_files(bucket_name: str, prefix: str) -> list[str]:
    try:
        client = storage.Client()
        bucket = client.bucket(CONFIG['bucket_name'])
        blobs_iter = client.list_blobs(bucket, prefix=CONFIG['gcs_prefix'])
        files = [blob.name for blob in blobs_iter]
        logger.info("Found %d objects under %s/%s", len(files), CONFIG['bucket_name'], CONFIG['gcs_prefix'])
        if not files:
            logger.info("No files found in the specified GCS bucket and prefix.")
            return []
        return files

    except Exception as e:
        logger.error("Error listing objects in %s/%s: %s", bucket_name, prefix, e)
        raise


def process_valid_files(valid_entries, gcs, bq_client):
    for item in valid_entries:
        # blob_path = item['path']
        # uri = f"gs://{CONFIG['bucket_name']}/{blob_path}"
        uri = f"gs://{CONFIG['bucket_name']}/{item}"
        logger.info("Downloading: %s", uri)

        with tempfile.NamedTemporaryFile("wb", suffix=".csv") as tmp:
            # gcs.download(bucket_name=CONFIG['bucket_name'], object_name=blob_path, filename=tmp.name)

            gcs.download(bucket_name=CONFIG['bucket_name'], object_name=item, filename=tmp.name)
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
            # logger.info("Upload successful for %s", blob_path)
            logger.info("Upload successful for %s", item)
        except Exception as e:
            logger.error("Upload failed for %s: %s", item, str(e))
            raise

def process_files_fn(**context):
    try:
        logger.info("Starting file processing.")
        gcs = GCSHook()
        bq_client = bigquery.Client(project=CONFIG['project_id'])
        sql = f"SELECT COUNT(*) AS cnt FROM `{JOBCONFIG['table_id']}`"
        cnt = next(bq_client.query(sql).result()).cnt
        mode = "Full load" if cnt == 0 else "Incremental"
        logger.info("%s mode ", mode)
        files_to_be_processed = list_gcs_files(CONFIG['bucket_name'], CONFIG['gcs_prefix'])
        if not files_to_be_processed:
            logger.info("No files to process.")
            return

        logger.info("%d files to process.", len(files_to_be_processed))
        process_valid_files(files_to_be_processed, gcs, bq_client)

    except Exception as e:
        logger.error("Error in process_files_fn: %s", str(e))
        raise

# ── DAG DEFINITION ─────────────────────────────────────────────────────
with DAG(
    dag_id="in_ya_unsubscribe_report_load",
    default_args=DEFAULT_ARGS,
    description="Incremental GCS→BQ load with first-run branch and sensor (hardcoded watermark)",
    schedule_interval="20 10 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["gcs", "bigquery", "production", "hardcoded"],
) as dag:

    decide = BranchPythonOperator(
        task_id="decide_next_step",
        python_callable=decide_next_step,
        trigger_rule="all_done",
        on_failure_callback=log_task_failure,
        dag=dag,
    )

    wait_for_file = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_files",
        bucket=CONFIG['bucket_name'],
        prefix=CONFIG['gcs_prefix'],
        poke_interval=CONFIG['poke_interval_seconds'],
        timeout=CONFIG['sensor_timeout_seconds'],
        mode="poke",
        on_failure_callback=log_task_failure,
        dag=dag,
    )

    process_files_direct = PythonOperator(
        task_id="process_files_direct",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
        dag=dag,
    )

    process_files_incremental = PythonOperator(
        task_id="process_files_incremental",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
        dag=dag,
    )

    decide >> [wait_for_file, process_files_direct]
    wait_for_file >> process_files_incremental
