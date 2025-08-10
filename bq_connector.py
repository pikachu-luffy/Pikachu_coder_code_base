import traceback
from datetime import datetime
from helpers.config import Config
from helpers.log import get_logger
from collections.abc import Callable
from google.cloud import bigquery, storage
from typing import List, Any, Dict, Optional
from helpers.log import get_logger, log_execution_time



class BQConnector:

    def __init__(self) -> None:
        self.config = Config()
        self.logger = get_logger()
        # self.notification_report = f'in_ya_{self.config.app.notification_report_prefix}'
        self.bq_client_data = bigquery.Client(project=self.config.gcp.gcp_bq_data_project_id)
        self.bq_project_id = self.config.gcp.gcp_bq_data_project_id
        self.bq_dataset_id = self.config.gcp.gcp_bq_dataset_id
        self.bq_notification_table = self.config.gcp.gcp_bq_notification_table
        self.bq_unsubscribe_table = self.config.gcp.gcp_bq_unsubscribe_table
        self.bq_lookup_table = self.config.gcp.gcp_bq_lookup_table
        self.bq_audit_table = self.config.gcp.gcp_bq_audit_table
    

    @log_execution_time(threshold=0.1)
    def get_bot_id_list(self):
        try:
            self.logger.info(f"Fetching bot_id_list from lookup table {self.bq_lookup_table}")
            fetch_botid_query = f"""SELECT bot_id FROM `{self.bq_lookup_table}` WHERE env_name = 'production'"""
            query_job = self.bq_client_data.query(fetch_botid_query)
            query_result = query_job.result()        
            bot_id_list = [row.bot_id for row in query_result]
            return bot_id_list
        except Exception as e:
            traceback.print_exc()
            self.error(f"Error in fetching bot_id_list: {e}")
            raise e
        
    
    @log_execution_time(threshold=0.1)
    def fetch_last_processed_epoch_ts(self, bot_prefix, report_name):
        """Fetch the highest processed GCS generation from audit table."""
        try:
            self.logger.debug(f"Querying last processed landing_time from {self.bq_audit_table}")
            query_max_generation = f"""SELECT MAX(landing_time) AS max_gen FROM `{self.bq_audit_table}` WHERE process_name = '{report_name}' and process_status='SUCCESS' and source_detail like '{bot_prefix}/%'"""
            query_result = next(self.bq_client_data.query(query_max_generation).result())    
            last_object_epoch = int(query_result.max_gen or 0)
            self.logger.info("Last processed object epoch: %s", last_object_epoch)
            return last_object_epoch
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error fetching last landing time of the object: {e}")
            raise e


    @log_execution_time(threshold=1.0)
    def insert_audit_records(self, records: list[dict]):
        """Append new audit records to audit table."""
        try:
            self.logger.debug(f"Inserting {len(records)} audit records")
            insertion_errors = self.bq_client_data.insert_rows_json(self.bq_audit_table, records)
            if insertion_errors:
                self.logger.error(f"Audit insert errors: {insertion_errors}")
                raise RuntimeError(insertion_errors)
            self.logger.info(f"Successfully inserted {len(records)} audit records")
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error inserting audit records: {e}")
            raise e
    
    @log_execution_time(threshold=1.0)
    def load_table_from_gcs_to_bq(self, gcs_uri, table_name, schema):   #name change for function
        try:
            self.logger.info(f"Loading table {table_name} from GCS to BigQuery")
            job_config=bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.CSV,
                            schema=schema,
                            skip_leading_rows=1,
                            allow_jagged_rows=True,
                            allow_quoted_newlines=True,
                            ignore_unknown_values=True,
                            autodetect=False,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                        
            bigquery_load_job = self.bq_client_data.load_table_from_uri(gcs_uri,table_name,job_config=job_config)
            bigquery_load_job.result()
            rows_loaded = bigquery_load_job.output_rows
            self.logger.info(f"Loaded {bigquery_load_job.output_rows} rows from {gcs_uri}")
            return rows_loaded
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error loading table from GCS to BigQuery: {e}")
            raise e