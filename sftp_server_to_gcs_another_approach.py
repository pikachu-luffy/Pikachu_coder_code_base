from datetime import datetime, timedelta
from airflow import DAG
import re
import os
import traceback
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("=== Starting SFTP to GCS Transfer DAG Initialization ===")

try:
    logger.info("Retrieving job configuration from Airflow Variables...")
    JOB_CONFIG = Variable.get("in_sftp_vendor_list", deserialize_json=True)
    logger.info(f"Successfully retrieved job config: {JOB_CONFIG}")
except Exception as e:
    logger.error(f"Failed to retrieve job configuration: {str(e)}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise

PIPELINE_CONFIG = {
    'sftp_vendor_list': JOB_CONFIG['sftp_vendor_list']
}

VENDOR_LIST_RAW = PIPELINE_CONFIG['sftp_vendor_list']
logger.info(f"Raw vendor list from config: {VENDOR_LIST_RAW} (type: {type(VENDOR_LIST_RAW)})")

# Process vendor list
if isinstance(VENDOR_LIST_RAW, str):
    logger.info("Processing vendor list as string...")
    VENDORS = [b.strip() for b in VENDOR_LIST_RAW.split(",") if b.strip()]
    logger.info(f"Parsed vendors from string: {VENDORS}")
elif isinstance(VENDOR_LIST_RAW, (list, tuple)):
    logger.info("Processing vendor list as list/tuple...")
    VENDORS = [str(b).strip() for b in VENDOR_LIST_RAW if str(b).strip()]
    logger.info(f"Processed vendors from list: {VENDORS}")
else:
    logger.warning(f"Unexpected vendor list type: {type(VENDOR_LIST_RAW)}. Setting empty vendor list.")
    VENDORS = []

logger.info(f"Final vendor list: {VENDORS} (Total: {len(VENDORS)} vendors)")

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

logger.info(f"DAG default arguments configured: {default_args}")

# DAG definition
dag = DAG(
    'in_sftp_to_gcs_transfer',
    default_args=default_args,
    description='Transfer files from SFTP to GCS with acknowledgment validation and GCS holding area',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

logger.info("DAG 'in_sftp_to_gcs_transfer' created successfully")

# Configuration
SFTP_CONN_ID = 'my_sftp_connection'
GCS_CONN_ID = 'google_cloud_default'
GCS_BUCKET = 'in-cde-sftp-file-transfer-dev-bkt'
SFTP_REMOTE_PATH = f'/D/DataHub/in/inbound'
GCS_OBJECT_PREFIX = 'sftp/inbound'
GCS_HOLDING_PREFIX = 'sftp/holding'  # New: GCS holding area prefix

logger.info("=== DAG Configuration ===")
logger.info(f"SFTP Connection ID: {SFTP_CONN_ID}")
logger.info(f"GCS Connection ID: {GCS_CONN_ID}")
logger.info(f"GCS Bucket: {GCS_BUCKET}")
logger.info(f"SFTP Remote Path: {SFTP_REMOTE_PATH}")
logger.info(f"GCS Object Prefix: {GCS_OBJECT_PREFIX}")
logger.info(f"GCS Holding Prefix: {GCS_HOLDING_PREFIX}")
logger.info("=========================")

def sanitize_for_task_id(name: str) -> str:
    """Sanitize string for use as Airflow task ID"""
    logger.debug(f"Sanitizing task ID for: '{name}'")
    s = re.sub(r'[^A-Za-z0-9]+', '_', name)
    s = re.sub(r'__+', '_', s).strip('_')
    result = s or 'brand'
    logger.debug(f"Sanitized task ID: '{name}' -> '{result}'")
    return result

def get_expected_ack_filename(data_filename: str) -> str:
    """
    Generate expected acknowledgment filename from data filename
    Example: abc_20250924jio.csv -> abc_20250924jio_ack.csv
    """
    logger.debug(f"Generating expected ack filename for: '{data_filename}'")
    name, ext = os.path.splitext(data_filename)
    expected_ack = f"{name}_ack{ext}"
    logger.debug(f"Expected ack filename: '{data_filename}' -> '{expected_ack}'")
    return expected_ack

def check_gcs_holding_and_recover(vendor: str, **context) -> None:
    """
    Check GCS holding area for files that now have ACK files on SFTP.
    Move those files back from GCS holding to SFTP for processing.
    """
    logger.info(f"=== Checking GCS holding area for recovery - Vendor: {vendor} ===")
    
    sftp_hook = None
    gcs_hook = None
    
    try:
        # Initialize connections
        logger.info("Initializing SFTP and GCS connections...")
        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        logger.info("Connections established successfully")
        
        vendor_path = f"{SFTP_REMOTE_PATH}/{vendor}"
        gcs_holding_prefix = f"{GCS_HOLDING_PREFIX}/{vendor}/"
        
        logger.info(f"Vendor SFTP path: {vendor_path}")
        logger.info(f"GCS holding prefix: gs://{GCS_BUCKET}/{gcs_holding_prefix}")
        
        # List files in GCS holding area
        logger.info("Listing files in GCS holding area...")
        try:
            holding_blobs = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=gcs_holding_prefix)
            holding_files = []
            for blob_name in holding_blobs:
                # Extract just the filename from the full blob path
                filename = blob_name.replace(gcs_holding_prefix, '')
                if filename and not filename.endswith('/'):  # Skip directory markers
                    holding_files.append(filename)
            
            logger.info(f"Found {len(holding_files)} files in GCS holding area: {holding_files}")
        except Exception as e:
            logger.info(f"No files in GCS holding area or error accessing: {str(e)}")
            holding_files = []
        
        if not holding_files:
            logger.info("No files in holding area to check for recovery")
            return
        
        # List current ACK files on SFTP
        logger.info("Checking for ACK files on SFTP...")
        try:
            sftp_files = sftp_hook.list_directory(vendor_path)
            current_ack_files = {f for f in sftp_files if '_ack.' in f}
            logger.info(f"Current ACK files on SFTP: {current_ack_files}")
        except Exception as e:
            logger.info(f"No files on SFTP or error accessing: {str(e)}")
            current_ack_files = set()
        
        # Check each holding file for corresponding ACK file
        recovered_files = []
        for holding_file in holding_files:
            expected_ack = get_expected_ack_filename(holding_file)
            logger.debug(f"Checking {holding_file} for ACK file: {expected_ack}")
            
            if expected_ack in current_ack_files:
                logger.info(f"🔄 RECOVERY CANDIDATE: {holding_file} now has ACK file: {expected_ack}")
                
                # Download from GCS holding to temp file
                gcs_blob_name = f"{gcs_holding_prefix}{holding_file}"
                temp_file_path = f"/tmp/recovery_{holding_file}"
                
                try:
                    logger.info(f"Step 1: Downloading from GCS holding: gs://{GCS_BUCKET}/{gcs_blob_name}")
                    gcs_hook.download(
                        bucket_name=GCS_BUCKET,
                        object_name=gcs_blob_name,
                        filename=temp_file_path
                    )
                    logger.debug("Download from GCS successful")
                    
                    # Upload to SFTP
                    sftp_file_path = f"{vendor_path}/{holding_file}"
                    logger.info(f"Step 2: Uploading to SFTP: {sftp_file_path}")
                    sftp_hook.store_file(sftp_file_path, temp_file_path)
                    logger.debug("Upload to SFTP successful")
                    
                    # Delete from GCS holding
                    logger.info(f"Step 3: Removing from GCS holding: {gcs_blob_name}")
                    gcs_hook.delete(bucket_name=GCS_BUCKET, object_name=gcs_blob_name)
                    logger.debug("Removal from GCS holding successful")
                    
                    # Clean up temp file
                    os.remove(temp_file_path)
                    logger.debug("Temp file cleaned up")
                    
                    recovered_files.append(holding_file)
                    logger.info(f"✅ Successfully recovered: {holding_file}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to recover {holding_file}: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # Clean up temp file if it exists
                    try:
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                    except:
                        pass
            else:
                logger.debug(f"No ACK file yet for: {holding_file}")
        
        if recovered_files:
            logger.info(f"🎉 Recovery complete! Recovered {len(recovered_files)} files: {recovered_files}")
        else:
            logger.info("No files were recovered from holding area")
            
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR in GCS holding recovery for vendor {vendor}: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        if sftp_hook:
            try:
                sftp_hook.close_conn()
                logger.debug("SFTP connection closed")
            except:
                pass

def validate_ack_files_and_cleanup(vendor: str, **context) -> None:
    """
    Validate that data files have corresponding ack files.
    Move files without ACK to GCS holding area instead of SFTP holding.
    Remove ack files after validation so they don't get transferred.
    """
    logger.info(f"=== Starting ACK file validation for vendor: {vendor} ===")
    
    sftp_hook = None
    gcs_hook = None
    
    try:
        logger.info(f"Initializing SFTP and GCS connections...")
        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
        logger.info("Connections established successfully")
        
        vendor_path = f"{SFTP_REMOTE_PATH}/{vendor}"
        logger.info(f"Vendor path: {vendor_path}")
        
        # Test SFTP connection
        logger.info("Testing SFTP connection...")
        try:
            sftp_hook.get_conn()
            logger.info("SFTP connection test successful")
        except Exception as e:
            logger.error(f"SFTP connection test failed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        # List all files in the vendor directory
        logger.info(f"Listing files in directory: {vendor_path}")
        try:
            all_files = sftp_hook.list_directory(vendor_path)
            logger.info(f"Successfully listed {len(all_files)} files in {vendor_path}")
            logger.info(f"Files found: {all_files}")
        except Exception as e:
            logger.error(f"Failed to list directory {vendor_path}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        if not all_files:
            logger.info(f"No files found for vendor {vendor}. Validation complete.")
            return
        
        # Separate data files and ack files
        logger.info("Categorizing files into data files and ack files...")
        data_files = []
        ack_files = set()
        
        for file in all_files:
            logger.debug(f"Processing file: {file}")
            if file.endswith('_ack.csv') or file.endswith('_ack.txt') or '_ack.' in file:
                ack_files.add(file)
                logger.debug(f"Categorized as ACK file: {file}")
            elif not file.startswith('.'):  # Ignore hidden files
                data_files.append(file)
                logger.debug(f"Categorized as DATA file: {file}")
            else:
                logger.debug(f"Ignored hidden file: {file}")
        
        logger.info(f"Categorization complete:")
        logger.info(f"  Data files ({len(data_files)}): {data_files}")
        logger.info(f"  ACK files ({len(ack_files)}): {list(ack_files)}")
        
        # Check each data file for corresponding ack file
        logger.info("Validating data files against ACK files...")
        files_to_hold = []
        validated_files = []
        
        for data_file in data_files:
            logger.debug(f"Validating data file: {data_file}")
            expected_ack = get_expected_ack_filename(data_file)
            
            if expected_ack in ack_files:
                validated_files.append(data_file)
                logger.info(f"✓ VALIDATED: {data_file} has corresponding ACK file: {expected_ack}")
            else:
                files_to_hold.append(data_file)
                logger.warning(f"✗ MISSING ACK: {data_file} is missing ACK file: {expected_ack}")
        
        logger.info(f"Validation summary:")
        logger.info(f"  Validated files ({len(validated_files)}): {validated_files}")
        logger.info(f"  Files without ACK ({len(files_to_hold)}): {files_to_hold}")
        
        # Move data files without ACK to GCS holding area
        if files_to_hold:
            logger.info(f"Moving {len(files_to_hold)} files without ACK to GCS holding area...")
            gcs_holding_prefix = f"{GCS_HOLDING_PREFIX}/{vendor}"
            logger.info(f"GCS holding prefix: gs://{GCS_BUCKET}/{gcs_holding_prefix}/")
            
            for file_to_hold in files_to_hold:
                source_path = f"{vendor_path}/{file_to_hold}"
                temp_path = f"/tmp/hold_{file_to_hold}"
                gcs_blob_name = f"{gcs_holding_prefix}/{file_to_hold}"
                
                logger.info(f"Moving file to GCS holding: {file_to_hold}")
                logger.debug(f"  SFTP source: {source_path}")
                logger.debug(f"  GCS destination: gs://{GCS_BUCKET}/{gcs_blob_name}")
                logger.debug(f"  Temp path: {temp_path}")
                
                try:
                    # Download from SFTP to temp file
                    logger.debug(f"Step 1: Downloading from SFTP: {source_path}")
                    sftp_hook.retrieve_file(source_path, temp_path)
                    logger.debug("Download from SFTP successful")
                    
                    # Upload to GCS holding area
                    logger.debug(f"Step 2: Uploading to GCS holding: {gcs_blob_name}")
                    gcs_hook.upload(
                        bucket_name=GCS_BUCKET,
                        object_name=gcs_blob_name,
                        filename=temp_path
                    )
                    logger.debug("Upload to GCS holding successful")
                    
                    # Delete from SFTP
                    logger.debug(f"Step 3: Deleting from SFTP: {source_path}")
                    sftp_hook.delete_file(source_path)
                    logger.debug("Deletion from SFTP successful")
                    
                    # Clean up temp file
                    logger.debug(f"Step 4: Cleaning up temp file: {temp_path}")
                    os.remove(temp_path)
                    logger.debug("Temp file cleaned up")
                    
                    logger.info(f"✅ Successfully moved {file_to_hold} to GCS holding area")
                    
                except Exception as e:
                    logger.error(f"❌ Error moving {file_to_hold} to GCS holding: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    # Clean up temp file if it exists
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                            logger.debug(f"Cleaned up temp file after error: {temp_path}")
                    except:
                        pass
        else:
            logger.info("No files need to be moved to GCS holding area")
        
        # Remove ack files so they don't get transferred by SFTPToGCSOperator
        if ack_files:
            logger.info(f"Removing {len(ack_files)} ACK files to prevent transfer...")
            for ack_file in ack_files:
                ack_path = f"{vendor_path}/{ack_file}"
                logger.debug(f"Removing ACK file: {ack_path}")
                
                try:
                    sftp_hook.delete_file(ack_path)
                    logger.info(f"✓ Successfully removed ACK file: {ack_file}")
                except Exception as e:
                    logger.warning(f"✗ Could not remove ACK file {ack_file}: {str(e)}")
                    logger.warning(f"Traceback: {traceback.format_exc()}")
        else:
            logger.info("No ACK files to remove")
        
        logger.info(f"=== Validation complete for vendor {vendor} ===")
        logger.info(f"Final summary:")
        logger.info(f"  Files ready for transfer: {len(validated_files)}")
        logger.info(f"  Files moved to GCS holding: {len(files_to_hold)}")
        logger.info(f"  ACK files removed: {len(ack_files)}")
        
        if validated_files:
            logger.info(f"Files ready for transfer: {validated_files}")
        else:
            logger.info("No validated files for transfer. SFTPToGCSOperator will handle empty directory gracefully.")
        
    except Exception as e:
        logger.error(f"=== CRITICAL ERROR in ACK validation for vendor {vendor} ===")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        if sftp_hook:
            try:
                logger.info("Closing SFTP connection...")
                sftp_hook.close_conn()
                logger.info("SFTP connection closed successfully")
            except Exception as e:
                logger.warning(f"Error closing SFTP connection: {str(e)}")

# Start and end markers for DAG clarity
logger.info("Creating start and end tasks...")
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)
logger.info("Start and end tasks created")

# If no VENDORS found, create a simple no-op branch and log via operator naming
if not VENDORS:
    logger.warning("No vendors configured! Creating placeholder task.")
    placeholder = EmptyOperator(task_id='no_VENDORS_configured', dag=dag)
    start >> placeholder >> end
    logger.info("DAG structure: start >> no_VENDORS_configured >> end")
else:
    logger.info(f"Creating tasks for {len(VENDORS)} vendors...")
    
    # For each configured vendor create recovery, validation and transfer tasks
    for i, vendor in enumerate(VENDORS, 1):
        logger.info(f"Creating tasks for vendor {i}/{len(VENDORS)}: {vendor}")
        
        safe_vendor = sanitize_for_task_id(vendor)
        logger.info(f"Safe vendor name for task IDs: {safe_vendor}")
        
        # Task 1: Check GCS holding area and recover files
        recovery_task_id = f"check_gcs_holding_{safe_vendor}"
        logger.info(f"Creating recovery task: {recovery_task_id}")
        
        recovery_task = PythonOperator(
            task_id=recovery_task_id,
            python_callable=check_gcs_holding_and_recover,
            op_kwargs={'vendor': vendor},
            dag=dag,
        )
        logger.info(f"✓ Recovery task created: {recovery_task_id}")
        
        # Task 2: Validate ack files and clean up
        validate_task_id = f"validate_ack_files_{safe_vendor}"
        logger.info(f"Creating validation task: {validate_task_id}")
        
        validate_task = PythonOperator(
            task_id=validate_task_id,
            python_callable=validate_ack_files_and_cleanup,
            op_kwargs={'vendor': vendor},
            dag=dag,
        )
        logger.info(f"✓ Validation task created: {validate_task_id}")
        
        # Task 3: Transfer validated files from SFTP to GCS
        transfer_task_id = f"transfer_sftp_to_gcs_{safe_vendor}"
        logger.info(f"Creating transfer task: {transfer_task_id}")
        
        source_path = f"{SFTP_REMOTE_PATH}/{vendor}/*"
        logger.info(f"Source path: {source_path}")

        destination_path = f"{GCS_OBJECT_PREFIX}/{vendor}"
        logger.info(f"Destination path: gs://{GCS_BUCKET}/{destination_path}")

        transfer_task = SFTPToGCSOperator(
            task_id=transfer_task_id,
            source_path=source_path,
            destination_bucket=GCS_BUCKET,
            destination_path=destination_path,
            sftp_conn_id=SFTP_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            move_object=True,
            dag=dag,
        )
        logger.info(f"✓ Transfer task created: {transfer_task_id}")

        # Set up task dependencies: recovery -> validation -> transfer
        start >> recovery_task >> validate_task >> transfer_task >> end
        logger.info(f"✓ Task dependencies set: start >> {recovery_task_id} >> {validate_task_id} >> {transfer_task_id} >> end")
    
    logger.info(f"All {len(VENDORS)} vendor task chains created successfully")

logger.info("=== DAG Initialization Complete ===")
logger.info(f"DAG Name: {dag.dag_id}")
logger.info(f"Schedule: {dag.schedule_interval}")
logger.info(f"Total Vendors: {len(VENDORS)}")
logger.info(f"Vendors: {VENDORS}")
logger.info("=====================================")