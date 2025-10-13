import os
import requests
import pendulum
import traceback
from helpers.config import Config
from importlib import import_module
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from typing import Dict, List, Optional, Any
from helpers.log import get_logger, log_execution_time
from logic.utilities import Utilities


class GCSToBQConnector:
    def __init__(self, dag_name: str,  execution_id: Optional[str] = None):
        self.dag_name = dag_name
        self.config = Config()
        self.logger = get_logger()
        self.execution_id = execution_id
        self.gcs_storage_client = storage.Client()
        self.utility = Utilities(self.dag_name)
        self.instance_id = self.get_cloud_run_instance_id()

        # Load DAG-specific configuration
        self.dag_config = self.config.get_dag_config(dag_name)
        if not self.dag_config:
            available_dags = self.config.list_available_dags()
            raise ValueError(f"DAG configuration not found for '{dag_name}'. Available DAGs: {available_dags}")
        # Dynamically load pipeline helper if configured
        self.bq_client_data = bigquery.Client(project=self.dag_config['target_project'])
        helper_info = self.dag_config.get('pipeline_helper', {})
        module_name = helper_info.get('module')
        class_name = helper_info.get('class')
        if module_name and class_name:
            module = import_module(module_name)
            helper_class = getattr(module, class_name)
            self.pipeline_helper = helper_class(self.dag_name)
            self.logger.info(f"Loaded pipeline helper: {module_name}.{class_name}")
        else:
            self.pipeline_helper = None
            self.logger.info("No pipeline helper configured; using generic processing.")
        self.logger.info(f'Initialized for {self.dag_name}')
    
    def get_cloud_run_instance_id(self):
        """
        Get Cloud Run instance ID.
        """
        try:
            cloudrun_job_execution_id = os.getenv('CLOUD_RUN_EXECUTION')
            if cloudrun_job_execution_id:
                self.logger.info(f"Found Cloud Run Job execution ID: {cloudrun_job_execution_id}")
                return cloudrun_job_execution_id
        except Exception as e:
            traceback.print_exc()
            self.logger.info('unable to fetch the cloud run instance ID.')

    

    def gcs_to_bigquery(self):
        """
        Main function to load data from GCS to BigQuery.
        This function checks for new files in GCS, processes them, and loads data into BigQuery.
        """
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            self.logger.info(f"Starting GCS to BigQuery processing for cloud run instance : {self.instance_id}")
            audit_rows = []
            prefix_list = self.utility.get_prefix_configurations()
            self.logger.info(f"Prefix_list  {prefix_list} for processing")
            for prefix_data in prefix_list:
                prefix = prefix_data.get('prefix')
                identifier = prefix_data.get('identifier')
                self.logger.info(f"Processing prefix={prefix}, identifier={identifier}")
                blob_list = self.utility.fetch_gcs_blobs_by_prefix(prefix)
                if identifier:
                    self.logger.info(f"Found {len(blob_list)} blobs to process for identifier={identifier}, prefix={prefix}")
                else:
                    self.logger.info(f"Found {len(blob_list)} blobs to process for prefix={prefix}")
                if not blob_list:
                    if identifier:
                        self.logger.info(f"No new files for identifier={identifier}, prefix={prefix}")
                        continue
                    else:
                        self.logger.info(f"No new files for prefix={prefix}")
                        continue
                for index, blob_info in enumerate(blob_list, start=1):
                    self.logger.info(f"Processing blob {index}/{len(blob_list)}: {blob_info['name']}")
                    file_processing_start_time = datetime.now(pendulum.UTC)
                    blob = self.gcs_storage_client.bucket(self.dag_config["gcs_bucket"]).get_blob(blob_info['name'])
                    total_rows_loaded = 0
                    file_load_status = 'FAILURE'
                    load_error_message = None
                    try:
                        pipeline_specifier = self.dag_config.get('process_method', 'process_csv_file')
                        if self.pipeline_helper:
                            local_path = getattr(self.pipeline_helper, pipeline_specifier)(blob, identifier)
                        else:
                            raise NotImplementedError("No pipeline helper to process file")
                        processed_blob_name = f'processed_files/{blob_info["name"]}'
                        processed_blob = self.gcs_storage_client.bucket(self.dag_config["gcs_bucket"]).blob(processed_blob_name)
                        processed_blob.upload_from_filename(local_path)
                        self.logger.info(f"Uploaded processed file to GCS: {processed_blob_name}")
                        gcs_uri = f"gs://{self.dag_config["gcs_bucket"]}/{processed_blob_name}"
                        schema = self.utility.build_bigquery_schema()
                        target_table = f"{self.dag_config['target_project']}.{self.dag_config['target_dataset']}.{self.dag_config['target_table']}"
                        total_rows_loaded = self.utility.import_data_from_gcs_to_bigquery(gcs_uri=gcs_uri,table_name=target_table,schema=schema)
                        if total_rows_loaded:
                            self.logger.info(f"Loaded {total_rows_loaded} rows for bot {identifier}")
                            self.utility.move_blob_to_processed_folder(blob_info['name'])
                            self.logger.info("Moved blob to processed_blobs folder.")
                            file_load_status = 'SUCCESS'
                            load_error_message = None
                    except Exception as e:
                        traceback.print_exc()
                        total_rows_loaded = 0
                        file_load_status = 'FAILURE'
                        load_error_message = str(e)
                        self.logger.error(f"Error processing blob {blob_info['name']}: {e}")
                    file_processing_end_time = datetime.now(pendulum.UTC)
                    audit_rows.append({
                        'execution_id': f'{self.dag_name}_{self.execution_id}',
                        'process_name': f'{self.instance_id}',
                        'source_name': f'{self.dag_config['source_name']}',   
                        'load_type': 'File',
                        'load_source_type': 'GCS',
                        'source_detail': f'{self.dag_config['gcs_bucket']}/{blob_info['name']}',
                        'target_table': self.dag_config['target_table'],
                        'process_start_time': file_processing_start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_end_time': file_processing_end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_status': file_load_status,
                        'rows_processed': total_rows_loaded,
                        'error_message': load_error_message,
                        'created_at': record_created_at,
                        'updated_at': datetime.now(pendulum.UTC).strftime('%Y-%m-%dT%H:%M:%S')
                    })
            if audit_rows:
                try:
                    self.utility.insert_audit_records(audit_rows)
                finally:
                    self.utility.cleanup_processed_files()
                    self.logger.info(f"Successfully processed {len(audit_rows)} files")
            else:
                self.logger.info("No audit records to insert, skipping insert_audit_records.")       
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in load_data_to_bigquery: {e}")
            raise e