from datetime import datetime, timedelta
from airflow import DAG
import re
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

JOB_CONFIG = Variable.get("in_sftp_vendor_list", deserialize_json=True)

PIPELINE_CONFIG = {
    'sftp_vendor_list': JOB_CONFIG['sftp_vendor_list']
}

VENDOR_LIST_RAW = PIPELINE_CONFIG['sftp_vendor_list']


if isinstance(VENDOR_LIST_RAW, str):
    # split on commas and strip spaces
    VENDORS = [b.strip() for b in VENDOR_LIST_RAW.split(",") if b.strip()]
elif isinstance(VENDOR_LIST_RAW, (list, tuple)):
    VENDORS = [str(b).strip() for b in VENDOR_LIST_RAW if str(b).strip()]
else:
    VENDORS = []
# Default arguments for the DAG
default_args = {
    'owner': 'mdlz_cde_in',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'in_sftp_to_gcs_transfer',
    default_args=default_args,
    description='Transfer files from SFTP to GCS',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# Configuration
SFTP_CONN_ID = 'my_sftp_connection'
GCS_CONN_ID = 'google_cloud_default'
GCS_BUCKET = 'in-cde-sftp-file-transfer-prd-bkt'
SFTP_REMOTE_PATH = f'/D/DataHub/in/inbound'
GCS_OBJECT_PREFIX = 'sftp/inbound'

def sanitize_for_task_id(name: str) -> str:
    # replace non-alphanumeric with underscore, collapse repeated underscores, trim leading/trailing underscores
    s = re.sub(r'[^A-Za-z0-9]+', '_', name)
    s = re.sub(r'__+', '_', s).strip('_')
    return s or 'brand'


# Start and end markers for DAG clarity
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# If no VENDORS found, create a simple no-op branch and log via operator naming
if not VENDORS:
    # create a single placeholder task to make the DAG explicit when no VENDORS are configured
    placeholder = EmptyOperator(task_id='no_VENDORS_configured', dag=dag)
    start >> placeholder >> end
else:
    # For each configured brand create an SFTP->GCS transfer task
    for vendor in VENDORS:
        safe_vendor = sanitize_for_task_id(vendor)
        task_id = f"transfer_sftp_to_gcs_{safe_vendor}"

        # Source: transfer everything under the brand folder
        # e.g. /D/DataHub/in/inbound/paytm/*
        source_path = f"{SFTP_REMOTE_PATH}/{vendor}/*"

        # Destination path will preserve brand folder under the base prefix
        # e.g. sftp/inbound/paytm/
        destination_path = f"{GCS_OBJECT_PREFIX}/{vendor}"

        transfer_task = SFTPToGCSOperator(
            task_id=task_id,
            source_path=source_path,
            destination_bucket=GCS_BUCKET,
            destination_path=destination_path,
            sftp_conn_id=SFTP_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            move_object=True,   # set False if you want to keep originals on SFTP
            dag=dag,
        )

        start >> transfer_task >> end