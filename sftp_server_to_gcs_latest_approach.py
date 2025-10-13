from datetime import datetime, timedelta
from airflow import DAG
import re
import os
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging

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
    description='Transfer files from SFTP to GCS with acknowledgment validation',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# Configuration
SFTP_CONN_ID = 'my_sftp_connection'
GCS_CONN_ID = 'google_cloud_default'
GCS_BUCKET = 'in-cde-sftp-file-transfer-dev-bkt'
SFTP_REMOTE_PATH = f'/D/DataHub/in/inbound'
GCS_OBJECT_PREFIX = 'sftp/inbound'

def sanitize_for_task_id(name: str) -> str:
    # replace non-alphanumeric with underscore, collapse repeated underscores, trim leading/trailing underscores
    s = re.sub(r'[^A-Za-z0-9]+', '_', name)
    s = re.sub(r'__+', '_', s).strip('_')
    return s or 'brand'

def get_expected_ack_filename(data_filename: str) -> str:
    """
    Generate expected acknowledgment filename from data filename
    Example: abc_20250924jio.csv -> abc_20250924jio_ack.csv
    """
    name, ext = os.path.splitext(data_filename)
    return f"{name}_ack{ext}"

def validate_ack_files_and_cleanup(vendor: str, **context) -> None:
    """
    Validate that data files have corresponding ack files.
    Remove files that don't have ack files to prevent SFTPToGCSOperator from transferring them.
    Also remove ack files after validation so they don't get transferred.
    """
    sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    vendor_path = f"{SFTP_REMOTE_PATH}/{vendor}"
    
    try:
        # List all files in the vendor directory
        all_files = sftp_hook.list_directory(vendor_path)
        logging.info(f"Found {len(all_files)} files in {vendor_path}: {all_files}")
        
        if not all_files:
            logging.info(f"No files found for vendor {vendor}")
            return
        
        # Separate data files and ack files
        data_files = []
        ack_files = set()
        
        for file in all_files:
            if file.endswith('_ack.csv') or file.endswith('_ack.txt') or '_ack.' in file:
                ack_files.add(file)
            elif not file.startswith('.'):  # Ignore hidden files
                data_files.append(file)
        
        logging.info(f"Data files: {data_files}")
        logging.info(f"Ack files: {ack_files}")
        
        # Check each data file for corresponding ack file
        files_to_remove = []
        validated_files = []
        
        for data_file in data_files:
            expected_ack = get_expected_ack_filename(data_file)
            if expected_ack in ack_files:
                validated_files.append(data_file)
                logging.info(f"✓ Found ack file for {data_file}: {expected_ack}")
            else:
                files_to_remove.append(data_file)
                logging.warning(f"✗ Missing ack file for {data_file}. Expected: {expected_ack}")
        
        # Remove data files that don't have ack files (temporarily move them to a holding area)
        if files_to_remove:
            # Create a holding directory for files without ack
            holding_path = f"{SFTP_REMOTE_PATH}/{vendor}_holding"
            try:
                sftp_hook.create_directory(holding_path)
            except:
                pass  # Directory might already exist
            
            for file_to_hold in files_to_remove:
                source_path = f"{vendor_path}/{file_to_hold}"
                dest_path = f"{holding_path}/{file_to_hold}"
                try:
                    # Move file to holding area
                    sftp_hook.retrieve_file(source_path, f"/tmp/{file_to_hold}")
                    sftp_hook.store_file(dest_path, f"/tmp/{file_to_hold}")
                    sftp_hook.delete_file(source_path)
                    os.remove(f"/tmp/{file_to_hold}")
                    logging.info(f"Moved {file_to_hold} to holding area (no ack file)")
                except Exception as e:
                    logging.error(f"Error moving {file_to_hold} to holding: {str(e)}")
        
        # Remove ack files so they don't get transferred by SFTPToGCSOperator
        for ack_file in ack_files:
            ack_path = f"{vendor_path}/{ack_file}"
            try:
                sftp_hook.delete_file(ack_path)
                logging.info(f"Removed ack file: {ack_file}")
            except Exception as e:
                logging.warning(f"Could not remove ack file {ack_file}: {str(e)}")
        
        logging.info(f"Validation complete. {len(validated_files)} files ready for transfer: {validated_files}")
        
        # If no validated files, skip the transfer task
        if not validated_files:
            logging.info(f"No validated files for vendor {vendor}. Transfer will be skipped.")
            # The SFTPToGCSOperator will handle empty directories gracefully
        
    except Exception as e:
        logging.error(f"Error validating ack files for vendor {vendor}: {str(e)}")
        raise
    finally:
        sftp_hook.close_conn()

# Start and end markers for DAG clarity
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# If no VENDORS found, create a simple no-op branch and log via operator naming
if not VENDORS:
    # create a single placeholder task to make the DAG explicit when no VENDORS are configured
    placeholder = EmptyOperator(task_id='no_VENDORS_configured', dag=dag)
    start >> placeholder >> end
else:
    # For each configured vendor create validation and SFTP->GCS transfer tasks
    for vendor in VENDORS:
        safe_vendor = sanitize_for_task_id(vendor)
        
        # Task 1: Validate ack files and clean up
        validate_task = PythonOperator(
            task_id=f"validate_ack_files_{safe_vendor}",
            python_callable=validate_ack_files_and_cleanup,
            op_kwargs={'vendor': vendor},
            dag=dag,
        )
        
        # Task 2: Your existing transfer logic (unchanged)
        transfer_task_id = f"transfer_sftp_to_gcs_{safe_vendor}"
        
        # Source: transfer everything under the brand folder
        # e.g. /D/DataHub/in/inbound/paytm/*
        source_path = f"{SFTP_REMOTE_PATH}/{vendor}/*"

        # Destination path will preserve brand folder under the base prefix
        # e.g. sftp/inbound/paytm/
        destination_path = f"{GCS_OBJECT_PREFIX}/{vendor}"

        transfer_task = SFTPToGCSOperator(
            task_id=transfer_task_id,
            source_path=source_path,
            destination_bucket=GCS_BUCKET,
            destination_path=destination_path,
            sftp_conn_id=SFTP_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            move_object=True,   # set False if you want to keep originals on SFTP
            dag=dag,
        )

        # Set up task dependencies: validate first, then transfer
        start >> validate_task >> transfer_task >> end