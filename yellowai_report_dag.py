import os
import json
import logging
import traceback
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator


VARS = json.loads(Variable.get("environment-variables"))
MARKET = VARS["MARKET"]
LOCATION = VARS["LOCATION"]
ENVIRONMENT = os.environ.get("environment", "")
SERVICES_PROJECT_ID = VARS["GCP_SERVICES_PROJECT_ID"]
YELLOWAI_EXECUTION_JOB_NAME = f"{MARKET}-yellowai-report-{ENVIRONMENT}-job"


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
    


with DAG(
    dag_id='in_yellowai_report_load_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='30 11 * * *',
    catchup=False,
) as dag:
    
    pipeline_start_task = EmptyOperator(
        task_id='start',
        on_failure_callback=log_task_failure
    )

    ya_data_load_task = CloudRunExecuteJobOperator(
        task_id = 'yellowai_report_load_job',
        project_id = SERVICES_PROJECT_ID,
        region = LOCATION,
        job_name = YELLOWAI_EXECUTION_JOB_NAME,
        on_failure_callback=log_task_failure
    )


    pipeline_end_task = EmptyOperator(
        task_id='end',
        on_failure_callback=log_task_failure
    )

    pipeline_start_task >> ya_data_load_task >> pipeline_end_task