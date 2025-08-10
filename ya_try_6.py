import logging
import tempfile
import pendulum
import pandas as pd
from airflow import DAG
from google.cloud import storage
from airflow.models import DagRun
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.session import provide_session
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor


JOBCONFIG = Variable.get("in_ya_unsub_rep_config", deserialize_json=True)


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


def log_task_failure(context):
    exc = context.get("exception")
    dag_id = context["dag"].dag_id
    task  = context["task_instance"].task_id
    ts    = context["execution_date"]
    logger.error("[DAG Failure] %s.%s @ %s → %s", dag_id, task, ts, exc)

@provide_session
def get_last_successful_run(dag_id, current_execution, session=None):
    dr = (
        session.query(DagRun)
        .filter(DagRun.dag_id==dag_id,
                DagRun.state=='success',
                DagRun.execution_date < current_execution)
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    return dr.execution_date if dr else None


def decide_next_step(**ctx):
    """Branch: full load if table empty, else sensor+incremental."""
    client = bigquery.Client(project=CONFIG['project_id'])
    row = next(client.query(f"SELECT COUNT(*) AS cnt FROM `{CONFIG['table_id']}`").result())
    cnt = row.cnt
    if cnt == 0:
        logger.info("Table empty (%s rows) → full load", cnt)
        return "process_files_direct"
    else:
        logger.info("Table has %s rows → incremental via sensor", cnt)
        # kick off both sensor and direct->incremental chain
        return "wait_for_new_files"


def list_gcs_files(gcs: GCSHook, since: pendulum.DateTime) -> list[str]:
    """List CSVs under prefix whose updated > since."""
    blobs = gcs.list(bucket_name=CONFIG['bucket_name'], prefix=CONFIG['gcs_prefix'])
    picks = []
    for name in blobs:
        if not name.lower().endswith(".csv"):
            continue
        try:
            meta = gcs.get_blob_metadata(bucket_name=CONFIG['bucket_name'], blob_name=name)
            ts   = meta.get("updated") or meta.get("timeCreated")
            updt = pendulum.parse(ts)
            if updt > since:
                picks.append(name)
                logger.info("Picked %s (updated %s)", name, ts)
        except Exception as e:
            logger.warning("Couldn't fetch metadata for %s: %s", name, e)
            # in full-load mode, since=epoch so this always includes it
    return picks


def list_csvs_after(since: pendulum.DateTime) -> list[str]:
    """
    List all CSVs under prefix whose blob.updated > since.
    If since is epoch, returns every CSV.
    """
    client = storage.Client()
    blobs  = client.list_blobs(CONFIG['bucket_name'], prefix=CONFIG['gcs_prefix'])
    picks  = []
    for blob in blobs:
        if not blob.name.lower().endswith(".csv"):
            continue
        updated = pendulum.instance(blob.updated).in_timezone("UTC")
        if updated > since:
            picks.append(blob.name)
            logger.info("→ selected %s (updated %s)", blob.name, updated)
    return picks


def process_files_fn(**context):
    """
    Downloads, stamps, and loads CSVs into BigQuery.
    - task_id "process_files_direct": full load (since=epoch)
    - task_id "process_files_incremental": incremental (since=execution_date)
    """
    ti        = context["ti"]
    exec_dt   = context["execution_date"]  # a pendulum datetime in UTC

    if ti.task_id == "process_files_direct":
        since = pendulum.datetime(1970,1,1, tz="UTC")
    else:
        last = get_last_successful_run(ti.dag_id, exec_dt)
        since = last if last else pendulum.datetime(1970,1,1, tz="UTC")

    logger.info("Cutoff (since) = %s", since)

    logger.info("Listing CSVs updated after %s", since)
    files = list_csvs_after(since)
    if not files:
        logger.info("No new files to process.")
        return

    logger.info("Will process %d file(s)", len(files))
    gcs      = GCSHook()
    bq       = bigquery.Client(project=CONFIG['project_id'])
    ist_zone = pendulum.timezone("Asia/Kolkata")
    ingestion_time = datetime.now(ist_zone)  

    for name in files:
        uri = f"gs://{CONFIG['bucket_name']}/{name}"
        logger.info("Downloading %s", uri)
        with tempfile.NamedTemporaryFile("wb", suffix=".csv") as tmp:
            gcs.download(bucket_name=CONFIG['bucket_name'], object_name=name, filename=tmp.name)
            df = pd.read_csv(tmp.name, dtype=str)

        df["ingestion_timestamp"] = ingestion_time

        job_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        logger.info("Loading %d rows into %s", len(df), CONFIG['table_id'])
        job = bq.load_table_from_dataframe(df, CONFIG['table_id'], job_config=job_cfg)
        job.result()
        logger.info("Successfully loaded %s", name)

# ── DAG DEFINITION ─────────────────────────────────────────────────────────
with DAG(
    dag_id="in_ya_unsubscribe_report_load",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 11 * * *",   
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
) as dag:

    decide = BranchPythonOperator(
        task_id="decide_next_step",
        python_callable=decide_next_step,
        on_failure_callback=log_task_failure,
    )

    wait_for_new = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_new_files",
        bucket=CONFIG['bucket_name'],
        prefix=CONFIG['gcs_prefix'],
        poke_interval=CONFIG['poke_interval_seconds'],
        timeout=CONFIG['sensor_timeout_seconds'],
        mode="poke",
        on_failure_callback=log_task_failure,
    )

    process_dir = PythonOperator(
        task_id="process_files_direct",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    process_inc = PythonOperator(
        task_id="process_files_incremental",
        python_callable=process_files_fn,
        trigger_rule="none_failed_min_one_success",
        on_failure_callback=log_task_failure,
    )

    # DAG graph
    decide >> process_dir       
    decide >> wait_for_new >> process_inc  
