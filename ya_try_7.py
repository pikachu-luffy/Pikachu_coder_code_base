import logging
import tempfile
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
from google.cloud import bigquery, storage, logging as cloud_logging
from airflow.operators.empty import EmptyOperator  # DummyOperator in newer versions is now EmptyOperator

# ── CONFIG ───────────────────────────────────────────────────────────────
JOBCONFIG   = Variable.get("in_ya_unsub_rep_config", deserialize_json=True)

CONFIG = {
    'project_id': JOBCONFIG['project_id'],
    'dataset': JOBCONFIG['dataset'],
    'main_table_id': JOBCONFIG['main_table_id'],     # need change 
    'bucket_name': JOBCONFIG['bucket_name'],
    'audit_table_id': JOBCONFIG['audit_table_id'],
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



cloud_logging_client = cloud_logging.Client(project=CONFIG['project_id'])
cloud_logging_client.setup_logging()
logger = logging.getLogger("unsubscribe_audit_dag")
logger.setLevel(logging.INFO)


def log_task_failure(context):    # change 
    dag    = context['dag'].dag_id
    task   = context['task_instance'].task_id
    ts     = context['execution_date']
    exc    = context.get('exception')
    logger.error("[DAG_FAIL] DAG=%s Task=%s ExecDate=%s Error=%s", dag, task, ts, exc)


# ── WATERMARK FUNCTIONS ──────────────────────────────────────────────────
def get_last_watermark():
    """Fetch the highest processed GCS generation from audit table."""
    logger.debug("Querying last watermark from %s", CONFIG['audit_table_id'])
    client = bigquery.Client(project=CONFIG['project_id'])
    sql = f"SELECT MAX(end_generation) AS max_gen FROM `{CONFIG['audit_table_id']}`"
    row = next(client.query(sql).result())    
    last = int(row.max_gen or 0)
    logger.info("Last watermark generation: %s", last)
    return last

def insert_audit_records(records: list[dict]):
    """Append new audit records to audit table."""
    logger.debug("Inserting %d audit records", len(records))
    client = bigquery.Client(project=CONFIG['project_id'])
    errors = client.insert_rows_json(CONFIG['audit_table_id'], records)
    if errors:
        logger.error("Audit insert errors: %s", errors)
        raise RuntimeError(errors)
    logger.info("Successfully inserted %d audit records", len(records))


# ── GCS LISTING ──────────────────────────────────────────────────────────
def list_new_blobs(since_generation: int) -> list[dict]:
    """Return list of GCS blobs with generation > since_generation."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(CONFIG['bucket_name'], prefix=CONFIG['gcs_prefix'])
    new_blobs = []
    for blob in blobs:
        if not blob.name.lower().endswith('.csv'):
            continue
        gen = int(blob.generation)
        if gen > since_generation:
            new_blobs.append({
                'name': blob.name,
                'generation': gen,
                'updated': blob.updated,
            })
            logger.info("New blob: %s gen=%s updated=%s", blob.name, gen, blob.updated)
    logger.info("Total new blobs: %d", len(new_blobs))
    return new_blobs


def decide_next_step_fn(**context):
    watermark = get_last_watermark()
    new_blobs = list_new_blobs(watermark)
    if new_blobs:
        logger.info("New files found → proceeding to incremental processing.")
        return "INCREMENTAL_LOAD"
    else:
        logger.info("No new files found → skipping processing.")
        return "SKIP_PROCESSING"

# ── PROCESSING TASK ─────────────────────────────────────────────────────
def process_files_fn(**context):
    ti      = context['ti']
    task_id = ti.task_id

    if task_id == 'FULL_LOAD':
        since_gen = 0
    else:
        since_gen = get_last_watermark()
    logger.info("Using watermark generation > %s", since_gen)

    blobs = list_new_blobs(since_gen)
    if not blobs:
        logger.info("No new files to process.")
        return

    gcs_hook = GCSHook()
    bq_client = bigquery.Client(project=CONFIG['project_id'])
    ist = pendulum.timezone('Asia/Kolkata')
    ingestion_time = datetime.now(ist)

    audit_records = []
    for blob in blobs:
        uri = f"gs://{CONFIG['bucket_name']}/{blob['name']}"
        logger.info("Processing %s", uri)

        with tempfile.NamedTemporaryFile("wb", suffix=".csv") as tmp:
            gcs_hook.download(bucket_name=CONFIG['bucket_name'], object_name=blob['name'], filename=tmp.name)
            df = pd.read_csv(tmp.name, dtype=str)

        df['ingestion_timestamp'] = ingestion_time

        job_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = bq_client.load_table_from_dataframe(df, CONFIG['main_table_id'], job_config=job_cfg)
        job.result()
        logger.info("Loaded %s rows from %s", len(df), blob['name'])

        audit_records.append({
            'file_name': blob['name'],
            'end_generation': blob['generation'],
            'updated_timestamp': blob['updated'].isoformat(),
            'ingestion_timestamp': ingestion_time.isoformat(),
            'processed_at': datetime.now(pendulum.UTC).isoformat(),
        })

    insert_audit_records(audit_records)


# ── DAG DEFINITION ───────────────────────────────────────────────────────
with DAG(
    dag_id="in_ya_unsubscribe_report_load",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 11 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Kolkata"),
    catchup=False,
) as dag:

    

    Initial_branch = BranchPythonOperator(
        task_id="DECIDE_LOAD_TYPE",
        python_callable=lambda **_: "FULL_LOAD" if get_last_watermark() == 0 else "CHECK_INCREMENTAL",
        on_failure_callback=log_task_failure,
        )

    Latest_file_arrived = BranchPythonOperator(
        task_id="CHECK_INCREMENTAL",
        python_callable=decide_next_step_fn,
        on_failure_callback=log_task_failure,
    )


    Bulk_load_trigger = PythonOperator(
        task_id="FULL_LOAD",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    Incremental_load_trigger = PythonOperator(
        task_id="INCREMENTAL_LOAD",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    
    Skip_task = EmptyOperator(task_id="SKIP_PROCESSING")

    

# updated flow
    Initial_branch >> Bulk_load_trigger
    Initial_branch >> Latest_file_arrived >> Incremental_load_trigger
    Latest_file_arrived >> Skip_task

