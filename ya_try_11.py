import time
import json
import logging
import tempfile
import pendulum
import traceback
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from airflow.operators.empty import EmptyOperator  
from airflow.operators.python import PythonOperator

# ── CONFIGURATION ───────────────────────────────────────────────────────────────
JOB_CONFIG = Variable.get("in_ya_unsub_rep_config", deserialize_json=True)

PIPELINE_CONFIG = {
    'project_id': JOB_CONFIG['project_id'],
    'dataset': JOB_CONFIG['dataset'],
    'table_id': JOB_CONFIG['table_id'],
    'bucket_name': JOB_CONFIG['bucket_name'],
    'audit_table_id': JOB_CONFIG['audit_table_id'],
    'bot_id_prefix': JOB_CONFIG['bot_id_prefix']
}

# DAG default arguments
DEFAULT_ARGS = {
    'owner': 'mdlz_cde_in',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}


def log_task_failure(context):
    """Log detailed error information on task failure."""
    try:
        task_exception = context.get('exception')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('execution_date')
        logging.error(
            f"[DAG: {dag_id}] Task Failure - Task ID: '{task_id}', Execution Date: '{execution_date}', Exception: {type(task_exception).__name__}: {task_exception}")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error in log_task_failure: {e}")
        raise e


def fetch_last_processed_epoch_ts(bot_prefix, context):
    """Fetch the highest processed GCS generation from audit table."""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        logging.debug(f"[DAG: {dag_id}] [Task: {task_id}] Querying last processed generation from {PIPELINE_CONFIG['audit_table_id']}")
        bigquery_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        query_max_generation = f"SELECT MAX(landing_time) AS max_gen FROM `{PIPELINE_CONFIG['audit_table_id']}` WHERE process_name= '{dag_id}' and load_status='SUCCESS' and ya_bot_id='{bot_prefix}'"
        query_result = next(bigquery_client.query(query_max_generation).result())    
        last_landing_time = int(query_result.max_gen or 0)
        logging.info(f"[DAG: {dag_id}] [Task: {task_id}] Last extracted landing_time: {last_landing_time}")
        return last_landing_time
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error fetching last watermark: {e}")
        raise e


def insert_audit_records(records: list[dict], context):
    """Append new audit records to audit table."""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        logging.debug(f"[DAG: {dag_id}] [Task: {task_id}] Inserting {len(records)} audit records")
        bq_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        insertion_errors = bq_client.insert_rows_json(PIPELINE_CONFIG['audit_table_id'], records)
        if insertion_errors:
            logging.error(f"[DAG: {dag_id}] [Task: {task_id}] Audit insert errors: {insertion_errors}")
            raise RuntimeError(insertion_errors)
        logging.info(f"[DAG: {dag_id}] [Task: {task_id}] Successfully inserted {len(records)} audit records")
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error inserting audit records: {e}")
        raise e


def get_blobs(landing_time: int, prefix, context) -> list[dict]:
    """Return list of GCS blobs with generation > since_generation."""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        gcs_storage_client = storage.Client()
        blob_list = gcs_storage_client.list_blobs(PIPELINE_CONFIG['bucket_name'], prefix=prefix)
        new_blob_list = []
        for blob in blob_list:
            if not (blob.name.lower().endswith(".csv") or blob.name.lower().endswith(".json")):
                continue
            blob_generation = int(blob.generation)
            if blob_generation > landing_time:
                new_blob_list.append({
                    'name': blob.name,
                    'generation': blob_generation,
                    'updated': blob.updated,
                })
                logging.info(f"[DAG: {dag_id}] [Task: {task_id}] New blob: {blob.name} gen={blob_generation} updated={blob.updated}")
        logging.info(f"[DAG: {dag_id}] [Task: {task_id}] Total new blobs: {len(new_blob_list)}")
        return new_blob_list
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error listing new blobs: {e}")  
        raise e  


def download_and_prepare_csv_from_json(blob, context):
    """Download JSON blob and convert to CSV format."""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        logging.info(f"[DAG: {dag_id}] [Task: {task_id}] Handling json files {blob.name}")
        content = blob.download_as_bytes()
        try:
            # Try loading as an array
            json_data = json.loads(content)
        except json.JSONDecodeError:
            # Try parsing as NDJSON (line-delimited)
            json_data = [json.loads(line) for line in content.decode('utf-8').splitlines() if line.strip()]
        
        if isinstance(json_data, dict):
            json_data = [json_data]
        df = pd.json_normalize(json_data)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(temp_file.name, index=False)
        return temp_file.name
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error processing blob {blob.name}: {e}")
        raise e

def load_data_to_bigquery(**context):
    """
    Main function to load data from GCS to BigQuery.
    This function checks for new files in GCS, processes them, and loads the data into BigQuery.
    It also updates the audit table with the latest processed generation and timestamps.
    """
    try:
        execution_run_id = context['run_id']
        dag_id = context['dag'].dag_id
        pipeline_load_start = datetime.now(pendulum.UTC)
        record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
        bigquery_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        audit_rows = []
        schema = [
                bigquery.SchemaField("phone", "STRING"),	
                bigquery.SchemaField("user_request", "STRING"),	
                bigquery.SchemaField("created_at", "STRING"),	
                bigquery.SchemaField("updated_at","STRING")
                ]
        bot_id_prefix_list = PIPELINE_CONFIG['bot_id_prefix']
        for each_bot_prefix in bot_id_prefix_list:
            logging.info("Processing bot prefix: %s", each_bot_prefix)
            last_processed_landing_time = fetch_last_processed_epoch_ts(each_bot_prefix, context)
            logging.info("Last processed epoch timestamp for bot %s: %s", each_bot_prefix, last_processed_landing_time)
            prefix = f"{each_bot_prefix}/bot_tables/unsubscribe_requests"
            blob_list = get_blobs(last_processed_landing_time, prefix, context)            
            if not blob_list:
                logging.info("No new files to process for bot %s. Exiting.", each_bot_prefix)
                continue

            for blob_info in blob_list:
                file_processing_start_time = datetime.now(pendulum.UTC)
                
                time.sleep(5)
                blob = storage.Client().bucket(PIPELINE_CONFIG['bucket_name']).get_blob(blob_info['name'])
                total_rows_loaded = 0
                file_load_status = 'FAILURE'
                load_error_message = None
                bigquery_load_job = None
                try:
                    if blob.name.lower().endswith('.json'):
                        logging.info("Processing JSON file: %s", blob.name)
                        local_path = download_and_prepare_csv_from_json(blob, context)
                        with open(local_path, 'rb') as file_obj:
                            job_config=bigquery.LoadJobConfig(
                                    source_format=bigquery.SourceFormat.CSV,
                                    schema=schema,
                                    skip_leading_rows=1,
                                    allow_jagged_rows=True,
                                    allow_quoted_newlines=True,
                                    ignore_unknown_values=True,
                                    autodetect=False,
                                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                            
                            bigquery_load_job = bigquery_client.load_table_from_file(
                                file_obj,
                                PIPELINE_CONFIG['table_id'],
                                job_config = job_config
                            )
                            bigquery_load_job.result()
                        logging.info("Loaded %d rows from JSON file %s", bigquery_load_job.output_rows, blob_info['name'])
                    else:
                        gcs_file_uri = f"gs://{PIPELINE_CONFIG['bucket_name']}/{blob_info['name']}"
                        logging.info(f"[DAG: {dag_id}] Loading {gcs_file_uri}")
                        bigquery_load_config = bigquery.LoadJobConfig(
                                source_format=bigquery.SourceFormat.CSV,
                                schema = schema,
                                skip_leading_rows = 1,
                                autodetect=False,
                                allow_jagged_rows=True,
                                allow_quoted_newlines=True,                    
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                        bigquery_load_job = bigquery_client.load_table_from_uri(gcs_file_uri, PIPELINE_CONFIG['table_id'], job_config=bigquery_load_config)
                        bigquery_load_job.result()
                        logging.info(f"[DAG: {dag_id}] Loaded {bigquery_load_job.output_rows} rows from {blob_info['name']}")
                    total_rows_loaded = bigquery_load_job.output_rows
                    if total_rows_loaded:
                        file_load_status = 'SUCCESS'
                    load_error_message = None
                except Exception as e:
                    traceback.print_exc()
                    total_rows_loaded = 0
                    file_load_status = 'FAILURE'
                    load_error_message = str(e)
                    logging.error(f"[DAG: {dag_id}] Error processing blob {blob_info['name']}: {e}")
                file_processing_end_time = datetime.now(pendulum.UTC)

                audit_rows.append({
                    'execution_id': execution_run_id,
                    'process_name': str(dag_id),
                    'source_name': str(PIPELINE_CONFIG['bucket_name']),
                    'load_type': 'File',
                    'load_source_type': 'GCS',
                    'source_detail': blob_info['name'],
                    'target_table': PIPELINE_CONFIG['table_id'],
                    'landing_time': blob_info['generation'],
                    'load_start_time': file_processing_start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'load_end_time': file_processing_end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'load_status': file_load_status,
                    'rows_loaded': total_rows_loaded,
                    'error_message': load_error_message,
                    'created_at': record_created_at,
                    'updated_at': datetime.now(pendulum.UTC).strftime('%Y-%m-%dT%H:%M:%S'),
                    'ya_bot_id': each_bot_prefix
                })

        insert_audit_records(audit_rows, context)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error in load_data_to_bigquery: {e}")
        raise e
    

with DAG(
    dag_id='in_ya_unsubscribe_report_load_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='30 11 * * *',
    catchup=False,
) as dag:

    pipeline_start_task = EmptyOperator(
        task_id='start',
        on_failure_callback=log_task_failure
    )

    data_load_task = PythonOperator(
        task_id='run_unsub_report_load',    
        python_callable=load_data_to_bigquery,
        on_failure_callback=log_task_failure,
    )

    pipeline_end_task = EmptyOperator(
        task_id='end',
        on_failure_callback=log_task_failure
    )

    pipeline_start_task >> data_load_task >> pipeline_end_task



