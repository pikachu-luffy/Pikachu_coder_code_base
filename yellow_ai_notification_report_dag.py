import json
import logging
import tempfile
import traceback
import pendulum
import pandas as pd
from airflow import DAG
from bson import json_util 
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
        logging.debug(f"[DAG: {dag_id}] [Task: {task_id}] Querying last processed landing_time from {PIPELINE_CONFIG['audit_table_id']}")
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


EXPECTED_COLUMNS = [
    "Id", "campaignId", "botId", "runId", "userId", "audienceId", "status",
    "statusDetail", "internalStatus", "comments", "messageId", "channelMsgId",
    "templateId", "customPayload", "userDetails", "currentCronTime", "source",
    "senderId", "workflowId", "updated", "created", "sentAt", "deliveredAt",
    "readAt", "repliedAt", "engagedAt", "usedPlainText", "errorResolution",
    "errorMessage", "rawError", "ipAddress", "sentBy", "name", "workflowStatus",
    "workflowNodeId", "workflowUserId", "quickReplyResponse", "workflowDetails",
    "notificationPostbackUrl", "subSource", "postbackAuthHeader", "cdpUserId",
    "smsUnits", "sessionStart", "sessionType", "sessionConversationId"
]


def extract_and_flatten(record: dict) -> dict:
    try:
        flat = {}

        # Handle _id which could be either a dict with $oid or an ObjectId directly
        _id_value = record.get("_id")
        if isinstance(_id_value, dict):
            flat["Id"] = _id_value.get("$oid", "")
        else:
            flat["Id"] = str(_id_value) if _id_value is not None else ""

        # Handle date fields
        for field in ["currentCronTime", "created", "updated", "sentAt", "deliveredAt", "readAt", "repliedAt"]:
            value = record.get(field)
            if isinstance(value, dict):
                flat[field] = value.get("$date", "")
            else:
                flat[field] = str(value) if value else ""

        # Process all other fields
        for key, value in record.items():
            if key in ["_id", "currentCronTime", "created", "updated", "sentAt", "deliveredAt", "readAt", "repliedAt"]:
                continue

            if isinstance(value, (dict, list)):
                flat[key] = json.dumps(value, ensure_ascii=False)
            else:
                flat[key] = value

        return flat
    except Exception as e:
        traceback.print_exc()
        logging.error(f"Error flattening record: {e}")
        raise e

def download_and_prepare_csv_from_json(blob):
    try:
        content = blob.download_as_bytes()
        records = []

        try:
            data = json_util.loads(content)
            if isinstance(data, dict):
                records = [data]
            elif isinstance(data, list):
                records = data
        except Exception:
            for line_number, line in enumerate(content.decode("utf-8").splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    record = json_util.loads(line)
                    records.append(record)
                except Exception as e:
                    logging.warning(f"Line {line_number} - Failed to parse JSON: {e}")

        if not records:
            raise ValueError("No valid records found in JSON blob.")

        flattened_records = [extract_and_flatten(r) for r in records]

        df = pd.DataFrame(flattened_records)

        # Ensure all expected columns are present
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = df[EXPECTED_COLUMNS]

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
        bot_id_prefix_list = PIPELINE_CONFIG['bot_id_prefix']
        for each_bot_prefix in bot_id_prefix_list:
            logging.info("Processing bot prefix: %s", each_bot_prefix)
            last_processed_epoch_ts = fetch_last_processed_epoch_ts(each_bot_prefix, context)
            logging.info("Last processed epoch timestamp for bot %s: %s", each_bot_prefix, last_processed_epoch_ts)
            prefix = f"{each_bot_prefix}/notifications"
            blob_list = get_blobs(last_processed_epoch_ts, prefix, context)
            if not blob_list:
                logging.info("No new files to process for bot %s. Exiting.", each_bot_prefix)
                continue
            for blob_info in blob_list:
                file_processing_start_time = datetime.now(pendulum.UTC)
                blob = storage.Client().bucket(PIPELINE_CONFIG['bucket_name']).get_blob(blob_info['name'])
                total_rows_loaded = 0
                file_load_status = 'FAILURE'
                load_error_message = None
                bigquery_load_job = None
                try:
                    if blob.name.lower().endswith('.json'):
                        logging.info("Processing JSON file: %s", blob.name)
                        local_path = download_and_prepare_csv_from_json(blob)
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
                        logging.info("Loading CSV file: %s", gcs_file_uri)
                        bigquery_load_config = bigquery.LoadJobConfig(
                            source_format = bigquery.SourceFormat.CSV,
                            skip_leading_rows = 1,
                            schema = schema,
                            autodetect=False,
                            allow_jagged_rows=True,
                            allow_quoted_newlines=True,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                        bigquery_load_job = bigquery_client.load_table_from_uri(gcs_file_uri, PIPELINE_CONFIG['table_id'], job_config=bigquery_load_config)
                        bigquery_load_job.result()
                        logging.info("Loaded %d rows from CSV file %s", bigquery_load_job.output_rows, blob_info['name'])
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

        if audit_rows:
            insert_audit_records(audit_rows, context)
        else:
            logging.info("No audit records to insert, skipping insert_audit_records.")
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