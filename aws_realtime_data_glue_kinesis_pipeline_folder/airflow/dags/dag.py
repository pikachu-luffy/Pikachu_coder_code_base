from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../opt/airflow/src')))

from insert_into_postgres import getting_and_inserting_data

logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    dag_id="stocks_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 22),
    max_active_runs=1
)

def start_job():
    logging.info("Starting the pipeline.")
    
def getting_and_inserting_data_to_postgres():
    logger.info("Fetching data from S3")
    getting_and_inserting_data()
    logger.info("data has been sent to Postgresql")
    
def end_job():
    logger.info("All process completed.")
    
start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

fetching_data_task = PythonOperator(
    task_id='fetch_data_job',
    python_callable=getting_and_inserting_data_to_postgres,
    dag=dag
)

end_task = PythonOperator(
    task_id= 'end_job',
    python_callable=end_job,
    dag=dag
)

start_task >> fetching_data_task >> end_task