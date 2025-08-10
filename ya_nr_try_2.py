import json
import logging
import tempfile
import traceback
import pendulum
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from airflow.operators.empty import EmptyOperator  
from airflow.operators.python import PythonOperator

# ── CONFIGURATION ───────────────────────────────────────────────────────────────
JOB_CONFIG = Variable.get("in_ya_notification_rep_config", deserialize_json=True)

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
        exception = context.get('exception')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        execution_date = context.get('execution_date')
        logging.error(
            f"[{dag_id}] Task Failure - Task ID: '{task_id}', Execution Date: '{execution_date}'"
        )
        logging.error(
            f"[{dag_id}] Exception: {type(exception).__name__}: {exception}"
        )
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error in log_task_failure: {e}")
        raise e


def fetch_last_processed_epoch_ts(bot_prefix, context):
    """Fetch the highest processed GCS generation from audit table."""
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        logging.debug("Querying last processed generation from %s", PIPELINE_CONFIG['audit_table_id'])
        bigquery_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        query_max_generation = f"SELECT MAX(landing_time) AS max_gen FROM `{PIPELINE_CONFIG['audit_table_id']}` WHERE process_name= '{dag_id}' and load_status='SUCCESS' and ya_bot_id='{bot_prefix}'"
        query_result = next(bigquery_client.query(query_max_generation).result())    
        last_generation_epoch = int(query_result.max_gen or 0)
        logging.info("Last processed generation: %s", last_generation_epoch)
        return last_generation_epoch
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error fetching last watermark: {e}")
        raise e
        

def insert_audit_records(records: list[dict]):
    """Append new audit records to audit table."""
    try:
        logging.debug("Inserting %d audit records", len(records))
        bigquery_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        insert_errors = bigquery_client.insert_rows_json(PIPELINE_CONFIG['audit_table_id'], records)
        if insert_errors:
            logging.error("Audit insert errors: %s", insert_errors)
            raise RuntimeError(insert_errors)
        logging.info("Successfully inserted %d audit records", len(records))
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error inserting audit records: {e}")
        raise e


def get_blobs(since_generation: int, prefix) -> list[dict]:
    """Return list of GCS blobs with generation > since_generation."""
    try:
        gcs_storage_client = storage.Client()
        blob_list = gcs_storage_client.list_blobs(PIPELINE_CONFIG['bucket_name'], prefix=prefix)
        new_blob_list = []
        for blob in blob_list:
            if not blob.name.lower().endswith('.csv') or blob.name.lower().endswith('.json'):
                continue
            blob_generation = int(blob.generation)
            if blob_generation > since_generation:
                new_blob_list.append({
                    'name': blob.name,
                    'generation': blob_generation,
                    'updated': blob.updated,
                })
                logging.info("New blob: %s gen=%s updated=%s", blob.name, blob_generation, blob.updated)
        logging.info("Total new blobs: %d", len(new_blob_list))
        return new_blob_list
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error listing new blobs: {e}")  
        raise e

def download_and_prepare_csv_from_json(blob):
    """Download JSON blob and convert to CSV format."""
    try:
        content = blob.download_as_bytes()
        json_data = json.loads(content)
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
        dag_process_name = context['dag'].dag_id
        pipeline_load_start = datetime.now(pendulum.UTC)
        record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
        bigquery_client = bigquery.Client(project=PIPELINE_CONFIG['project_id'])
        audit_rows = []
        bot_id_prefix_list = PIPELINE_CONFIG['bot_id_prefix']
        for each_bot_prefix in bot_id_prefix_list:
            logging.info("Processing bot prefix: %s", each_bot_prefix)
            last_processed_epoch_ts = fetch_last_processed_epoch_ts(each_bot_prefix, context)
            logging.info("Last processed epoch timestamp for bot %s: %s", each_bot_prefix, last_processed_epoch_ts)
            prefix = f"{each_bot_prefix}/notifications"
            blob_list = get_blobs(last_processed_epoch_ts, prefix)
            if not blob_list:
                logging.info("No new files to process for bot %s. Exiting.", each_bot_prefix)
                continue
            for blob_info in blob_list:
                file_processing_start_time = datetime.now(pendulum.UTC)
                try:
                    gcs_file_uri = f"gs://{PIPELINE_CONFIG['bucket_name']}/{blob_info['name']}"
                    logging.info("Loading %s", gcs_file_uri)
                    schema = [
                    bigquery.SchemaField("Id",		"STRING"),
                    bigquery.SchemaField("campaignId",		"STRING"),
                    bigquery.SchemaField("botId",		"STRING"),
                    bigquery.SchemaField("runId",		"STRING"),
                    bigquery.SchemaField("userId",		"STRING"),
                    bigquery.SchemaField("audienceId",		"STRING"),
                    bigquery.SchemaField("status",		"STRING"),
                    bigquery.SchemaField("statusDetail",		"STRING"),
                    bigquery.SchemaField("internalStatus",		"STRING"),
                    bigquery.SchemaField("comments",		"STRING"),
                    bigquery.SchemaField("messageId",		"STRING"),
                    bigquery.SchemaField("channelMsgId",		"STRING"),
                    bigquery.SchemaField("templateId",		"STRING"),
                    bigquery.SchemaField("customPayload",		"STRING"),
                    bigquery.SchemaField("userDetails",		"STRING"),
                    bigquery.SchemaField("currentCronTime",		"STRING"),
                    bigquery.SchemaField("source",		"STRING"),
                    bigquery.SchemaField("senderId",		"STRING"),
                    bigquery.SchemaField("workflowId",		"STRING"),
                    bigquery.SchemaField("updated",		"STRING"),
                    bigquery.SchemaField("created",		"STRING"),
                    bigquery.SchemaField("sentAt",		"STRING"),
                    bigquery.SchemaField("deliveredAt",		"STRING"),
                    bigquery.SchemaField("readAt",		"STRING"),
                    bigquery.SchemaField("repliedAt",		"STRING"),
                    bigquery.SchemaField("engagedAt",		"STRING"),
                    bigquery.SchemaField("usedPlainText",		"STRING"),
                    bigquery.SchemaField("errorResolution",		"STRING"),
                    bigquery.SchemaField("errorMessage",		"STRING"),
                    bigquery.SchemaField("rawError",		"STRING"),
                    bigquery.SchemaField("ipAddress",		"STRING"),
                    bigquery.SchemaField("sentBy",		"STRING"),
                    bigquery.SchemaField("name",		"STRING"),
                    bigquery.SchemaField("workflowStatus",		"STRING"),
                    bigquery.SchemaField("workflowNodeId",		"STRING"),
                    bigquery.SchemaField("workflowUserId",		"STRING"),
                    bigquery.SchemaField("quickReplyResponse",		"STRING"),
                    bigquery.SchemaField("workflowDetails",		"STRING"),
                    bigquery.SchemaField("notificationPostbackUrl",		"STRING"),
                    bigquery.SchemaField("subSource",		"STRING"),
                    bigquery.SchemaField("postbackAuthHeader",		"STRING"),
                    bigquery.SchemaField("cdpUserId",		"STRING"),
                    bigquery.SchemaField("smsUnits",		"STRING"),
                    bigquery.SchemaField("sessionStart",		"STRING"),
                    bigquery.SchemaField("sessionType",		"STRING"),
                    bigquery.SchemaField("sessionConversationId",		"STRING")
                    ]
                    bigquery_load_config = bigquery.LoadJobConfig(
                        source_format = bigquery.SourceFormat.CSV,
                        skip_leading_rows = 1,
                        schema = schema,
                        allow_jagged_rows=True,
                        allow_quoted_newlines=True,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                    bigquery_load_job = bigquery_client.load_table_from_uri(gcs_file_uri, PIPELINE_CONFIG['table_id'], job_config=bigquery_load_config)
                    bigquery_load_job.result()
                    logging.info("Loaded %d rows from %s", bigquery_load_job.output_rows, blob_info['name'])
                    total_rows_loaded = bigquery_load_job.output_rows
                    if total_rows_loaded:
                        file_load_status = 'SUCCESS'
                    load_error_message = None
                except Exception as e:
                    traceback.print_exc()
                    total_rows_loaded = 0
                    file_load_status = 'FAILURE'
                    load_error_message = str(e)
                    logging.error(f"Error processing blob {blob_info['name']}: {e}")
                file_processing_end_time = datetime.now(pendulum.UTC)

                audit_rows.append({
                    'execution_id': execution_run_id,
                    'process_name': str(dag_process_name),
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

        insert_audit_records(audit_rows)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error in load_data_to_bigquery: {e}")
        raise e
    

with DAG(
    dag_id='in_ya_notification_report_load_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='30 11 * * *',
    catchup=False,
) as dag:

    pipeline_start_task = EmptyOperator(
        task_id='start',
        on_failure_callback=log_task_failure
    )

    data_load_task = PythonOperator(
        task_id='run_notfication_report_load',    
        python_callable=load_data_to_bigquery,
        on_failure_callback=log_task_failure,
    )

    pipeline_end_task = EmptyOperator(
        task_id='end',
        on_failure_callback=log_task_failure
    )

    pipeline_start_task >> data_load_task >> pipeline_end_task