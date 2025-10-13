import os
import json
import uuid
import asyncio
import pendulum
import tempfile
import traceback
import pandas as pd
from helpers.config import Config
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from typing import Dict, List, Optional, Tuple, Any
from api.id_service_connector import IdServiceConnector
from api.id_service_helper import fetch_mdlz_ids_parallel
from helpers.log import get_logger, log_execution_time


class SftpReport:
    def __init__(self, dag_name: str):
        self.config = Config()
        self.logger = get_logger()
        self.dag_name = dag_name
        self.dag_config = self.config.get_dag_config(dag_name)
        if not self.dag_config:
            available_dags = self.config.list_available_dags()
            raise ValueError(f"DAG configuration not found for '{dag_name}'. Available DAGs: {available_dags}")
        self.bq_client_data = bigquery.Client(project=self.dag_config['target_project'])
        self.logger.info(f'Initialized for #### {self.dag_name}')
    
    
    def build_bigquery_schema(self) -> List[bigquery.SchemaField]:
        """Build BigQuery schema from configuration."""
        schema_config = self.dag_config.get('error_table_schema', [])
        schema = []
        
        for field_config in schema_config:
            schema.append(bigquery.SchemaField(
                field_config['name'],
                field_config['type'],
                mode=field_config.get('mode', 'NULLABLE')
            ))
        
        return schema

    def get_identifier_prefix(self) -> List[dict]:
        try:
            """SFTP specific implementation"""
            config = self.dag_config
            prefix_list = []
            sftp_brand_list = config.get('brand_list', [])
            self.logger.info("brand list is as such {sftp_brand_list}")
            if sftp_brand_list:
                for brand in sftp_brand_list:
                    prefix = f'{config.get('gcs_prefix', '')}/{brand}'
                    prefix_list.append({'prefix': prefix})
                self.logger.info("configured prefix list is as such")
                return prefix_list
            else:
                raise ValueError("No prefix list provided in DAG configuration")
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in generating prefix configurations: {e}")
            raise e
        
    def load_error_records(self, unexpected_columns: List[str], df, blob, record_created_at: str) -> None:
        try:
            unexpected_data_list = []
            process_name = "SFTP"
            for index, row in df.iterrows():
                row_unexpected_data = {col: row[col] for col in unexpected_columns}
                unexpected_data_list.append(json.dumps(row_unexpected_data, default=str))
            error_data = {
            'process_name': [process_name] * len(df),
            'phone': df['phone'].astype(str).tolist(),
            'data': unexpected_data_list,
            'total_records': [len(unexpected_data_list)] * len(df),
            'file_name': [blob.name] * len(df),
            'created_at': [record_created_at] * len(df),
            'updated_at': [datetime.now(pendulum.UTC).strftime('%Y-%m-%dT%H:%M:%S')] * len(df)
            }
            error_df = pd.DataFrame(error_data)
            target_error_table = f"{self.dag_config['target_project']}.{self.dag_config['target_dataset']}.{self.dag_config['target_error_table']}"
            error_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            error_df.to_csv(error_tmp.name, header=False, index=False)
            error_tmp.close()
            bigquery_load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                                        allow_jagged_rows=True,
                                                        allow_quoted_newlines=True,
                                                        ignore_unknown_values=True,
                                                        autodetect=False)
            error_schema = self.build_bigquery_schema()
            bigquery_load_config.schema = error_schema
            with open(error_tmp.name, 'rb') as error_file:
                load_error_job = self.bq_client_data.load_table_from_file(error_file, target_error_table, job_config=bigquery_load_config)
            error_loaded_data = load_error_job.result()
            self.logger.info(f"Error table data loaded: {error_loaded_data}")

        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in loading error records: {e}")
            raise e
    
    def transform_to_json_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform dataframe to have only mdlzID and sftpData (JSON) columns."""
        try:
            self.logger.info("Transforming dataframe to JSON format with mdlzID and sftpData columns")
            
            # Create a new dataframe with only mdlzID and sftpData columns
            transformed_data = []
            
            for index, row in df.iterrows():
                # Extract mdlzID
                mdlz_id = row.get('mdlzID', None)
                
                # Create sftpData JSON object with all columns except mdlzID
                sftp_data = {}
                for col in df.columns:
                    if col != 'mdlzID':
                        # Convert pandas NaN/None to None for JSON serialization
                        value = row[col]
                        if pd.isna(value):
                            sftp_data[col] = None
                        else:
                            sftp_data[col] = value
                
                # Convert sftpData to JSON string
                sftp_data_json = json.dumps(sftp_data, default=str)
                
                transformed_data.append({
                    'mdlzID': mdlz_id,
                    'sftpData': sftp_data_json
                })
            
            transformed_df = pd.DataFrame(transformed_data)
            self.logger.info(f"Successfully transformed {len(transformed_df)} records to JSON format")
            
            return transformed_df
        except Exception as e:
            self.logger.error(f"Error transforming dataframe to JSON format: {e}")
            raise e
    
    def process_csv_file(self, blob:storage.Blob, identifier: str) -> Optional[str]:
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            self.logger.info("Processing CSV file from SFTP: {blob.name} for {identifier} identifier")
            content = blob.download_as_bytes()
            df = pd.read_csv(pd.io.common.BytesIO(content), low_memory=False)
            column_mappings = self.dag_config.get('column_mappings', {})
            if column_mappings:
                df.rename(columns=column_mappings, inplace=True)
                self.logger.info(f"Applied column mappings: {column_mappings}")
            raw_file_df_columns = df.columns.to_list()
            self.logger.info(f"CSV file columns: {raw_file_df_columns}")
            expected_schema = self.dag_config.get('expected_columns', [])
            if expected_schema:
                available_columns = [col for col in expected_schema if col in df.columns]
                unexpected_columns = [col for col in raw_file_df_columns if col not in expected_schema]
                main_df = df[available_columns] if available_columns else pd.DataFrame()
                self.logger.info(f"Filtered to expected columns: {available_columns}")
                if unexpected_columns:
                    self.logger.warning(f"Unexpected columns: {unexpected_columns}")
                    self.load_error_records(unexpected_columns, df, blob, record_created_at)
                phone_column = self.dag_config.get('phone_column')
                if phone_column and phone_column in main_df.columns:
                    enriched_df =  self.enrich_with_mdlz_ids(main_df, phone_column, blob.name)
                final_columns = self.dag_config.get('final_columns', [])
                for col in final_columns:
                    if col not in enriched_df.columns:
                        enriched_df[col] = None
                if final_columns:
                    enriched_df = enriched_df[final_columns]
                    self.logger.info(f"Reordered columns to match final schema: {final_columns}")
                transformed_df = self.transform_to_json_format(enriched_df)
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
                transformed_df.to_csv(tmp.name, index=False)
                self.logger.info(f"Processed CSV saved to temporary file: {tmp.name}")
                return tmp.name
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in processing CSV file: {e}")
            raise e
    
    def enrich_with_mdlz_ids(self, df: pd.DataFrame, phone_column: str, blob_name: str) -> pd.DataFrame:
        """Enrich dataframe with Mondelez IDs based on phone numbers."""
        try:
            self.logger.info(f"Fetching mdlzIds for {phone_column} in {blob_name}")
            df['normalized_phone'] = '+91' + df[phone_column].astype(str).str.lstrip('+')
            phones = df['normalized_phone'].dropna().astype(str).unique().tolist()
            
            if phones:
                phone_to_mdlzid = asyncio.run(fetch_mdlz_ids_parallel(phones))
                df['mdlzID'] = df['normalized_phone'].map(phone_to_mdlzid)
                self.logger.info(f"Successfully enriched {len(phones)} unique phone numbers with mdlzIDs")
            else:
                df['mdlzID'] = None
                self.logger.info("No phone numbers found for mdlzID enrichment")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error enriching with mdlz IDs: {e}")
            df['normalized_phone'] = None
            df['mdlzID'] = None
            return df
        


