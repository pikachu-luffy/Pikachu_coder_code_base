import os
import json
import asyncio
import pendulum
import tempfile
import traceback
import pandas as pd
from helpers.config import Config
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from typing import Dict, List, Optional, Tuple, Any
from api.id_service_connector import IdServiceConnector, PersonaFields
from api.id_service_helper import fetch_mdlz_ids_parallel, fetch_mdlz_ids_parallel_personas
from helpers.log import get_logger, log_execution_time


class SftpReport:
    def __init__(self, dag_name: str, execution_id: Optional[str] = None):
        self.config = Config()
        self.logger = get_logger()
        self.dag_name = dag_name
        self.dag_config = self.config.get_dag_config(dag_name)
        if not self.dag_config:
            available_dags = self.config.list_available_dags()
            raise ValueError(f"DAG configuration not found for '{dag_name}'. Available DAGs: {available_dags}")
        self.execution_id = execution_id or os.getenv('EXECUTION_ID')
        self.logger.info(f"execution_id: {self.execution_id}")
        self.bq_client_data = bigquery.Client(project=self.dag_config['target_project'])
        self.logger.info(f'Initialized for #### {self.dag_name}')
    

    def add_channel_level_consent(self, data: Dict, phone_value: Optional[str], email_value: Optional[str], consent_timestamp: Optional[str] = None) -> Dict:
        """
        Add channel level consent fields based on phone/email availability.
        
        Args:
            data: Dictionary containing the row data
            phone_value: Cleaned phone number (original format)
            email_value: Cleaned email address
            consent_timestamp: Timestamp from consent_timestamp column
            
        Returns:
            Dictionary with added consent fields
        """
        # Create a copy to avoid modifying original data
        enriched_data = data.copy()
        
        # Get consent timestamp from the data if not provided
        if not consent_timestamp:
            consent_timestamp = data.get('consent_timestamp')
        
        # Convert timestamp to ISO format if needed
        formatted_timestamp = None
        if consent_timestamp:
            try:
                # Handle different timestamp formats
                if isinstance(consent_timestamp, str):
                    if 'UTC' in consent_timestamp:
                        # Format: "2025-08-30 18:28:41 UTC"
                        dt = datetime.strptime(consent_timestamp, '%Y-%m-%d %H:%M:%S UTC')
                        formatted_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    else:
                        # Try to parse other formats
                        try:
                            dt = datetime.fromisoformat(consent_timestamp.replace('Z', '+00:00'))
                            formatted_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        except Exception as e:
                            formatted_timestamp = consent_timestamp
                            self.logger.info(f"Error parsing consent timestamp '{consent_timestamp}': {e}")
                else:
                    formatted_timestamp = str(consent_timestamp)
            except Exception as e:
                self.logger.warning(f"Error parsing consent timestamp '{consent_timestamp}': {e}")
                formatted_timestamp = consent_timestamp
        
        # Determine consent logic based on phone/email availability
        has_phone = phone_value is not None and str(phone_value).strip() != ''
        has_email = email_value is not None and str(email_value).strip() != ''
        
        if has_phone and has_email:
            # Both phone and email provided - all consents are true
            enriched_data['mdlz_email_consent_status'] = "true"
            enriched_data['mdlz_email_consent_status_ts'] = formatted_timestamp
            enriched_data['mdlz_sms_consent_status'] = "true"
            enriched_data['mdlz_sms_consent_status_ts'] = formatted_timestamp
            enriched_data['mdlz_whatsapp_consent_status'] = "true"
            enriched_data['mdlz_whatsapp_consent_status_ts'] = formatted_timestamp
            self.logger.debug(f"Added consent for both phone and email: phone={phone_value}, email={email_value}")
            
        elif has_phone and not has_email:
            # Only phone provided - SMS and WhatsApp consent true, Email null
            enriched_data['mdlz_email_consent_status'] = None
            enriched_data['mdlz_email_consent_status_ts'] = None
            enriched_data['mdlz_sms_consent_status'] = "true"
            enriched_data['mdlz_sms_consent_status_ts'] = formatted_timestamp
            enriched_data['mdlz_whatsapp_consent_status'] = "true"
            enriched_data['mdlz_whatsapp_consent_status_ts'] = formatted_timestamp
            self.logger.debug(f"Added consent for phone only: phone={phone_value}")
            
        elif has_email and not has_phone:
            # Only email provided - Email consent true, SMS and WhatsApp null
            enriched_data['mdlz_email_consent_status'] = "true"
            enriched_data['mdlz_email_consent_status_ts'] = formatted_timestamp
            enriched_data['mdlz_sms_consent_status'] = None
            enriched_data['mdlz_sms_consent_status_ts'] = None
            enriched_data['mdlz_whatsapp_consent_status'] = None
            enriched_data['mdlz_whatsapp_consent_status_ts'] = None
            self.logger.debug(f"Added consent for email only: email={email_value}")
            
        else:
            # Neither phone nor email provided - all consents null
            enriched_data['mdlz_email_consent_status'] = None
            enriched_data['mdlz_email_consent_status_ts'] = None
            enriched_data['mdlz_sms_consent_status'] = None
            enriched_data['mdlz_sms_consent_status_ts'] = None
            enriched_data['mdlz_whatsapp_consent_status'] = None
            enriched_data['mdlz_whatsapp_consent_status_ts'] = None
            self.logger.debug("No phone or email provided - all consent fields set to null")
        
        return enriched_data
    
    
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
        

    # def load_error_records(self, error_records: List[Dict], blob: storage.Blob) -> None:
    #     """Load error records to BigQuery with new schema structure."""
    #     PROCESSED_FLAG = 0
    #     try:
    #         if not error_records:
    #             self.logger.info("No error records to load")
    #             return
                
    #         self.logger.info(f"Loading {len(error_records)} error records to BigQuery")
            
    #         # Prepare error data for BigQuery
    #         error_data = {
    #             'execution_id': [],
    #             'phone': [],
    #             'email': [],  # Added email column
    #             'file_name': [],
    #             'error_detail': [],
    #             'data': [],
    #             'processed_flag':[]
    #         }
            
    #         current_timestamp = datetime.now(pendulum.UTC).strftime('%Y-%m-%d %H:%M:%S')
            
    #         for error_record in error_records:
    #             error_data['execution_id'].append(self.execution_id)
    #             error_data['phone'].append(error_record.get('phone'))
    #             error_data['email'].append(error_record.get('email'))  # Added email field
    #             error_data['file_name'].append(blob.name)
    #             error_data['error_detail'].append(error_record.get('error_detail'))
    #             error_data['data'].append(json.dumps(error_record.get('data'), default=str))
    #             error_data['processed_flag'].append(PROCESSED_FLAG)
            
    #         error_df = pd.DataFrame(error_data)
    #         target_error_table = f"{self.dag_config['target_project']}.{self.dag_config['target_dataset']}.{self.dag_config['target_error_table']}"
            
    #         # Create temporary file for BigQuery load
    #         error_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    #         error_df.to_csv(error_tmp.name, header=False, index=False)
    #         error_tmp.close()
            
    #         # Configure BigQuery load job
    #         bigquery_load_config = bigquery.LoadJobConfig(
    #             write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #             allow_jagged_rows=True,
    #             allow_quoted_newlines=True,
    #             ignore_unknown_values=True,
    #             autodetect=False
    #         )
    #         error_schema = self.build_bigquery_schema()
    #         bigquery_load_config.schema = error_schema
            
    #         # Load data to BigQuery
    #         with open(error_tmp.name, 'rb') as error_file:
    #             load_error_job = self.bq_client_data.load_table_from_file(
    #                 error_file, target_error_table, job_config=bigquery_load_config
    #             )
    #         error_loaded_data = load_error_job.result()
    #         self.logger.info(f"Error table data loaded successfully: {error_loaded_data}")
            
    #         # Clean up temporary file
    #         os.unlink(error_tmp.name)

    #     except Exception as e:
    #         traceback.print_exc()
    #         self.logger.error(f"Error in loading error records: {e}")
    #         raise e

    def load_error_records(self, error_records: List[Dict], blob: storage.Blob) -> None:
        """Load error records to BigQuery with new schema structure including mdlzID."""
        PROCESSED_FLAG = 0
        try:
            if not error_records:
                self.logger.info("No error records to load")
                return
                
            self.logger.info(f"Loading {len(error_records)} error records to BigQuery")
            
            # Prepare error data for BigQuery
            error_data = {
                'execution_id': [],
                'mdlzID': [],  # Added mdlzID column
                'phone': [],
                'email': [],
                'file_name': [],
                'error_detail': [],
                'data': [],
                'processed_flag': []
            }
            
            current_timestamp = datetime.now(pendulum.UTC).strftime('%Y-%m-%d %H:%M:%S')
            
            # Track mdlzID statistics for logging
            records_with_mdlzid = 0
            
            for error_record in error_records:
                error_data['execution_id'].append(self.execution_id)
                
                # Handle mdlzID - can be None for records where ID service failed
                mdlz_id = error_record.get('mdlzID')
                error_data['mdlzID'].append(mdlz_id)
                if mdlz_id:
                    records_with_mdlzid += 1
                
                error_data['phone'].append(error_record.get('phone'))
                error_data['email'].append(error_record.get('email'))
                error_data['file_name'].append(blob.name)
                error_data['error_detail'].append(error_record.get('error_detail'))
                error_data['data'].append(json.dumps(error_record.get('data'), default=str))
                error_data['processed_flag'].append(PROCESSED_FLAG)
            
            # Log mdlzID population statistics
            records_without_mdlzid = len(error_records) - records_with_mdlzid
            self.logger.info(f"Error records mdlzID stats: {records_with_mdlzid} with mdlzID, {records_without_mdlzid} without mdlzID")
            
            error_df = pd.DataFrame(error_data)
            target_error_table = f"{self.dag_config['target_project']}.{self.dag_config['target_dataset']}.{self.dag_config['target_error_table']}"
            
            # Create temporary file for BigQuery load
            error_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            error_df.to_csv(error_tmp.name, header=False, index=False)
            error_tmp.close()
            
            # Configure BigQuery load job
            bigquery_load_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                allow_jagged_rows=True,
                allow_quoted_newlines=True,
                ignore_unknown_values=True,
                autodetect=False
            )
            error_schema = self.build_bigquery_schema()
            bigquery_load_config.schema = error_schema
            
            # Load data to BigQuery
            with open(error_tmp.name, 'rb') as error_file:
                load_error_job = self.bq_client_data.load_table_from_file(
                    error_file, target_error_table, job_config=bigquery_load_config
                )
            error_loaded_data = load_error_job.result()
            self.logger.info(f"Error table data loaded successfully: {error_loaded_data}")
            
            # Clean up temporary file
            os.unlink(error_tmp.name)

        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in loading error records: {e}")
            raise e
        
    # def check_schema_mismatch_and_filter(self, df: pd.DataFrame, blob: storage.Blob) -> Tuple[List[Dict], pd.DataFrame]:
    #     """Check for schema mismatch at row level and return error records and valid dataframe."""
    #     error_records = []
    #     expected_schema = self.dag_config.get('expected_columns', [])
        
    #     if not expected_schema:
    #         return error_records, df
            
    #     raw_file_columns = df.columns.tolist()
    #     unexpected_columns = [col for col in raw_file_columns if col not in expected_schema]
        
    #     self.logger.info(f"Schema validation: Expected columns: {len(expected_schema)}, File columns: {len(raw_file_columns)}")
    #     if unexpected_columns:
    #         self.logger.info(f"Unexpected columns found: {unexpected_columns}")
        
    #     # If no unexpected columns, return original dataframe
    #     if not unexpected_columns:
    #         self.logger.info("No schema mismatch found. All rows are valid.")
    #         return error_records, df
        
    #     # Check each row for unexpected column data
    #     valid_row_indices = []
    #     phone_column = self.dag_config.get('phone_column', 'phone')
    #     email_column = self.dag_config.get('email_column', 'email')  # Added email column config
        
    #     for index, row in df.iterrows():
    #         # Check if this row has non-null values in unexpected columns
    #         has_unexpected_data = False
    #         unexpected_data_columns = []
            
    #         for col in unexpected_columns:
    #             if col in row and not pd.isna(row[col]) and str(row[col]).strip() != '':
    #                 has_unexpected_data = True
    #                 unexpected_data_columns.append(col)
            
    #         if has_unexpected_data:
    #             # This row has data in unexpected columns - send to error table
    #             row_data = {}
    #             for col in df.columns:
    #                 value = row[col]
    #                 if pd.isna(value):
    #                     row_data[col] = None
    #                 else:
    #                     row_data[col] = value
                
    #             # Get phone and email for error record
    #             phone_value = row.get(phone_column, None)
    #             if pd.isna(phone_value):
    #                 phone_value = None
                
    #             email_value = row.get(email_column, None)
    #             if pd.isna(email_value):
    #                 email_value = None
                
    #             error_detail = f"Row contains unexpected columns with data: {unexpected_data_columns}. Expected schema has {len(expected_schema)} columns but file has {len(raw_file_columns)} columns."
                
    #             error_record = {
    #                 'mdlzID': None,  # No mdlzID available at this stage
    #                 'phone': phone_value,
    #                 'email': email_value,  # Added email field
    #                 'error_detail': error_detail,
    #                 'data': row_data
    #             }
    #             error_records.append(error_record)
    #             self.logger.debug(f"Row {index} marked as error due to unexpected data in columns: {unexpected_data_columns}")
    #         else:
    #             # This row is valid - keep it for main processing
    #             valid_row_indices.append(index)
        
    #     # Create dataframe with only valid rows
    #     if valid_row_indices:
    #         valid_df = df.iloc[valid_row_indices].copy()
    #         # Remove unexpected columns from valid dataframe
    #         columns_to_keep = [col for col in df.columns if col in expected_schema]
    #         valid_df = valid_df[columns_to_keep]
    #         self.logger.info(f"Found {len(valid_row_indices)} valid rows out of {len(df)} total rows")
    #     else:
    #         valid_df = pd.DataFrame()
    #         self.logger.warning("No valid rows found after schema validation")
        
    #     if error_records:
    #         self.logger.warning(f"Found {len(error_records)} rows with schema mismatch")
        
    #     return error_records, valid_df

    # def check_schema_mismatch_and_filter(self, df: pd.DataFrame, blob: storage.Blob) -> Tuple[List[Dict], pd.DataFrame]:
    #     """Check for schema mismatch at row level and return error records and valid dataframe."""
    #     error_records = []
    #     expected_schema = self.dag_config.get('expected_columns', [])
        
    #     if not expected_schema:
    #         return error_records, df
            
    #     raw_file_columns = df.columns.tolist()
    #     unexpected_columns = [col for col in raw_file_columns if col not in expected_schema]
        
    #     self.logger.info(f"Schema validation: Expected columns: {len(expected_schema)}, File columns: {len(raw_file_columns)}")
    #     if unexpected_columns:
    #         self.logger.info(f"Unexpected columns found: {unexpected_columns}")
        
    #     # If no unexpected columns, return original dataframe
    #     if not unexpected_columns:
    #         self.logger.info("No schema mismatch found. All rows are valid.")
    #         return error_records, df
        
    #     # Check each row for unexpected column data
    #     valid_row_indices = []
    #     phone_column = self.dag_config.get('phone_column', 'phone')
    #     email_column = self.dag_config.get('email_column', 'email')  # Added email column config
        
    #     for index, row in df.iterrows():
    #         # Check if this row has non-null values in unexpected columns
    #         has_unexpected_data = False
    #         unexpected_data_columns = []
            
    #         for col in unexpected_columns:
    #             if col in row and not pd.isna(row[col]) and str(row[col]).strip() != '':
    #                 has_unexpected_data = True
    #                 unexpected_data_columns.append(col)
            
    #         if has_unexpected_data:
    #             # This row has data in unexpected columns - send to error table
    #             row_data = {}
    #             for col in df.columns:
    #                 value = row[col]
    #                 if pd.isna(value):
    #                     row_data[col] = None
    #                 else:
    #                     row_data[col] = value
                
    #             # Get phone and email for error record
    #             phone_value = self.clean_phone_number(row.get(phone_column))
    #             email_value = self.clean_email_address(row.get(email_column))
                
    #             # Add channel level consent to error record data
    #             row_data_with_consent = self.add_channel_level_consent(
    #                 row_data, phone_value, email_value
    #             )
                
    #             error_detail = f"Row contains unexpected columns with data: {unexpected_data_columns}. Expected schema has {len(expected_schema)} columns but file has {len(raw_file_columns)} columns."
                
    #             error_record = {
    #                 'mdlzID': None,  # No mdlzID available at this stage
    #                 'phone': phone_value,
    #                 'email': email_value,  # Added email field
    #                 'error_detail': error_detail,
    #                 'data': row_data_with_consent
    #             }
    #             error_records.append(error_record)
    #             self.logger.debug(f"Row {index} marked as error due to unexpected data in columns: {unexpected_data_columns}")
    #         else:
    #             # This row is valid - keep it for main processing
    #             valid_row_indices.append(index)
        
    #     # Create dataframe with only valid rows
    #     if valid_row_indices:
    #         valid_df = df.iloc[valid_row_indices].copy()
    #         # Remove unexpected columns from valid dataframe
    #         columns_to_keep = [col for col in df.columns if col in expected_schema]
    #         valid_df = valid_df[columns_to_keep]
    #         self.logger.info(f"Found {len(valid_row_indices)} valid rows out of {len(df)} total rows")
    #     else:
    #         valid_df = pd.DataFrame()
    #         self.logger.warning("No valid rows found after schema validation")
        
    #     if error_records:
    #         self.logger.warning(f"Found {len(error_records)} rows with schema mismatch")
        
    #     return error_records, valid_df

    def check_schema_mismatch_and_filter(self, df: pd.DataFrame, blob: storage.Blob) -> Tuple[List[Dict], pd.DataFrame]:
        """Check for schema mismatch at row level and return error records and valid dataframe."""
        error_records = []
        file_expected_schema = self.dag_config.get('file_expected_columns', [])
        expected_schema = self.dag_config.get('expected_columns', [])
        
        if not expected_schema:
            return error_records, df
            
        raw_file_columns = df.columns.tolist()
        unexpected_columns = [col for col in raw_file_columns if col not in expected_schema]
        
        self.logger.info(f"Schema validation: Expected columns: {len(expected_schema)}, File columns: {len(raw_file_columns)}")
        if unexpected_columns:
            self.logger.info(f"Unexpected columns found: {unexpected_columns}")
        
        # If no unexpected columns, return original dataframe
        if not unexpected_columns:
            self.logger.info("No schema mismatch found. All rows are valid.")
            return error_records, df
        
        # Check each row for unexpected column data
        valid_row_indices = []
        schema_error_rows = []  # Store rows with schema mismatch for ID service processing
        phone_column = self.dag_config.get('phone_column', 'phone')
        email_column = self.dag_config.get('email_column', 'email')
        
        for index, row in df.iterrows():
            # Check if this row has non-null values in unexpected columns
            has_unexpected_data = False
            unexpected_data_columns = []
            
            for col in unexpected_columns:
                if col in row and not pd.isna(row[col]) and str(row[col]).strip() != '':
                    has_unexpected_data = True
                    unexpected_data_columns.append(col)
            
            if has_unexpected_data:
                # This row has data in unexpected columns - prepare for error processing
                row_data = {}
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        row_data[col] = None
                    else:
                        row_data[col] = value
                
                # Get phone and email for error record
                phone_value = self.clean_phone_number(row.get(phone_column))
                email_value = self.clean_email_address(row.get(email_column))
                
                error_detail = f"Row contains unexpected columns with data: {unexpected_data_columns}. Expected schema has {len(file_expected_schema)} columns but file has {len(raw_file_columns)} columns."
                
                # Store schema error row for batch ID service processing
                schema_error_rows.append({
                    'index': index,
                    'phone': phone_value,
                    'email': email_value,
                    'error_detail': error_detail,
                    'row_data': row_data
                })
                
                self.logger.debug(f"Row {index} marked as error due to unexpected data in columns: {unexpected_data_columns}")
            else:
                # This row is valid - keep it for main processing
                valid_row_indices.append(index)
        
        # Process schema error rows to get mdlzIDs
        if schema_error_rows:
            self.logger.info(f"Fetching mdlzIDs for {len(schema_error_rows)} schema mismatch records")
            
            try:
                # Prepare personas for ID service batch processing
                personas_for_id_service = []
                row_to_persona_map = {}  # Map persona index to schema error row
                
                for i, error_row in enumerate(schema_error_rows):
                    phone_value = error_row['phone']
                    email_value = error_row['email']
                    
                    # Normalize phone if available
                    normalized_phone = None
                    if phone_value:
                        if not phone_value.startswith('+91'):
                            normalized_phone = f'+91{phone_value}'
                        else:
                            normalized_phone = phone_value
                    
                    # Create persona for ID service
                    if normalized_phone or email_value:
                        persona = PersonaFields(phone=normalized_phone, email=email_value)
                        personas_for_id_service.append(persona)
                        row_to_persona_map[len(personas_for_id_service) - 1] = i
                
                # Fetch mdlzIDs for schema error records
                if personas_for_id_service:
                    id_success, id_errors = asyncio.run(fetch_mdlz_ids_parallel_personas(personas_for_id_service))
                    self.logger.info(f"Schema mismatch ID service results: {len(id_success)} successful, {len(id_errors)} errors")
                    
                    # Apply results back to schema error rows
                    for persona_idx, schema_row_idx in row_to_persona_map.items():
                        persona = personas_for_id_service[persona_idx]
                        error_row = schema_error_rows[schema_row_idx]
                        
                        mdlz_id = None
                        
                        # Phone-first priority logic
                        if persona.phone and persona.phone in id_success:
                            mdlz_id = id_success[persona.phone]
                            self.logger.debug(f"Schema error row {error_row['index']}: Phone {persona.phone} succeeded -> {mdlz_id}")
                        elif persona.email and persona.email in id_success:
                            mdlz_id = id_success[persona.email]
                            self.logger.debug(f"Schema error row {error_row['index']}: Email {persona.email} succeeded -> {mdlz_id}")
                        
                        # Add channel level consent to error record data
                        row_data_with_consent = self.add_channel_level_consent(
                            error_row['row_data'], error_row['phone'], error_row['email']
                        )
                        
                        # Create final error record with mdlzID (if available)
                        error_record = {
                            'mdlzID': mdlz_id,  # This will be populated if ID service succeeded
                            'phone': error_row['phone'],
                            'email': error_row['email'],
                            'error_detail': error_row['error_detail'],
                            'data': row_data_with_consent
                        }
                        error_records.append(error_record)
                        
                        if mdlz_id:
                            self.logger.debug(f"Schema mismatch record for row {error_row['index']} enriched with mdlzID: {mdlz_id}")
                        else:
                            self.logger.debug(f"Schema mismatch record for row {error_row['index']} could not get mdlzID")
                else:
                    # No valid personas for ID service - create error records without mdlzID
                    for error_row in schema_error_rows:
                        row_data_with_consent = self.add_channel_level_consent(
                            error_row['row_data'], error_row['phone'], error_row['email']
                        )
                        
                        error_record = {
                            'mdlzID': None,
                            'phone': error_row['phone'],
                            'email': error_row['email'],
                            'error_detail': error_row['error_detail'],
                            'data': row_data_with_consent
                        }
                        error_records.append(error_record)
                        
            except Exception as e:
                self.logger.error(f"Error fetching mdlzIDs for schema mismatch records: {e}")
                traceback.print_exc()
                
                # Fallback: Create error records without mdlzID
                for error_row in schema_error_rows:
                    row_data_with_consent = self.add_channel_level_consent(
                        error_row['row_data'], error_row['phone'], error_row['email']
                    )
                    
                    error_record = {
                        'mdlzID': None,
                        'phone': error_row['phone'],
                        'email': error_row['email'],
                        'error_detail': f"{error_row['error_detail']} (ID service error: {str(e)})",
                        'data': row_data_with_consent
                    }
                    error_records.append(error_record)
        
        # Create dataframe with only valid rows
        if valid_row_indices:
            valid_df = df.iloc[valid_row_indices].copy()
            # Remove unexpected columns from valid dataframe
            columns_to_keep = [col for col in df.columns if col in expected_schema]
            valid_df = valid_df[columns_to_keep]
            self.logger.info(f"Found {len(valid_row_indices)} valid rows out of {len(df)} total rows")
        else:
            valid_df = pd.DataFrame()
            self.logger.warning("No valid rows found after schema validation")
        
        if error_records:
            self.logger.warning(f"Found {len(error_records)} rows with schema mismatch")
            # Log summary of mdlzID population for schema errors
            records_with_mdlzid = len([r for r in error_records if r.get('mdlzID')])
            self.logger.info(f"Schema mismatch records: {records_with_mdlzid} with mdlzID, {len(error_records) - records_with_mdlzid} without mdlzID")
        
        return error_records, valid_df
    

    # def transform_to_json_format(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """Transform dataframe to have only mdlzID and sftpData (JSON) columns."""
    #     try:
    #         self.logger.info("Transforming dataframe to JSON format with mdlzID and sftpData columns")
            
    #         # Create a new dataframe with only mdlzID and sftpData columns
    #         transformed_data = []
            
    #         for index, row in df.iterrows():
    #             # Extract mdlzID
    #             mdlz_id = row.get('mdlzID', None)
                
    #             # Create sftpData JSON object with all columns except mdlzID
    #             sftp_data = {}
    #             for col in df.columns:
    #                 if col != 'mdlzID':
    #                     # Convert pandas NaN/None to None for JSON serialization
    #                     value = row[col]
    #                     if pd.isna(value):
    #                         sftp_data[col] = None
    #                     else:
    #                         sftp_data[col] = value
                
    #             # Convert sftpData to JSON string
    #             sftp_data_json = json.dumps(sftp_data, default=str)
                
    #             transformed_data.append({
    #                 'mdlzID': mdlz_id,
    #                 'sftpData': sftp_data_json
    #             })
            
    #         transformed_df = pd.DataFrame(transformed_data)
    #         self.logger.info(f"Successfully transformed {len(transformed_df)} records to JSON format")
            
    #         return transformed_df
    #     except Exception as e:
    #         self.logger.error(f"Error transforming dataframe to JSON format: {e}")
    #         raise e

    def transform_to_json_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform dataframe to have only mdlzID and sftpData (JSON) columns with channel level consent."""
        try:
            self.logger.info("Transforming dataframe to JSON format with mdlzID and sftpData columns")
            
            # Get phone and email column names
            phone_column = self.dag_config.get('phone_column', 'phone')
            email_column = self.dag_config.get('email_column', 'email')
            
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
                
                # Get phone and email values for consent logic
                phone_value = self.clean_phone_number(row.get(phone_column))
                email_value = self.clean_email_address(row.get(email_column))
                
                # Add channel level consent fields
                sftp_data_with_consent = self.add_channel_level_consent(
                    sftp_data, phone_value, email_value
                )
                
                # Convert sftpData to JSON string
                sftp_data_json = json.dumps(sftp_data_with_consent, default=str)
                
                transformed_data.append({
                    'mdlzID': mdlz_id,
                    'sftpData': sftp_data_json
                })
            
            transformed_df = pd.DataFrame(transformed_data)
            self.logger.info(f"Successfully transformed {len(transformed_df)} records to JSON format with channel level consent")
            
            return transformed_df
        except Exception as e:
            self.logger.error(f"Error transforming dataframe to JSON format: {e}")
            raise e
    
    def clean_phone_number(self, phone_value: Any) -> Optional[str]:
        """Clean phone number to remove .0 suffix and handle various formats."""
        if phone_value is None or pd.isna(phone_value):
            return None
        
        phone_str = str(phone_value).strip()
        
        # Handle empty strings
        if not phone_str or phone_str.lower() in ['nan', 'none', '']:
            return None
        
        # Remove .0 suffix if present
        if phone_str.endswith('.0'):
            phone_str = phone_str[:-2]
        
        # Validate that it contains digits
        if not any(c.isdigit() for c in phone_str):
            return None
        
        return phone_str if phone_str else None
    

    def clean_email_address(self, email_value: Any) -> Optional[str]:
        """Clean email address to handle NaN and formatting issues."""
        if email_value is None or pd.isna(email_value):
            return None
        
        email_str = str(email_value).strip()
        
        # Handle empty strings
        if not email_str or email_str.lower() in ['nan', 'none', '']:
            return None
        
        return email_str if email_str else None

    def read_csv_with_proper_dtypes(self, content: bytes) -> pd.DataFrame:
        """Read CSV with proper data types to prevent phone number float conversion."""
        phone_column = self.dag_config.get('phone_column', 'phone')
        email_column = self.dag_config.get('email_column', 'email')
        
        # Create dtype dictionary to force string types for phone/email columns
        dtype_dict = {}
        
        # First, read just the header to get column names
        header_df = pd.read_csv(pd.io.common.BytesIO(content), nrows=0)
        
        # Set string dtype for phone and email columns that exist in the file
        for col in header_df.columns:
            if col == phone_column or col == email_column:
                dtype_dict[col] = str
        
        self.logger.info(f"Reading CSV with dtype specifications: {dtype_dict}")
        
        # Read the full file with proper dtypes
        df = pd.read_csv(pd.io.common.BytesIO(content), dtype=dtype_dict, low_memory=False)
        
        # Additional cleaning for phone and email columns
        if phone_column in df.columns:
            df[phone_column] = df[phone_column].apply(self.clean_phone_number)
            self.logger.info(f"Cleaned phone column: {phone_column}")
        
        if email_column in df.columns:
            df[email_column] = df[email_column].apply(self.clean_email_address)
            self.logger.info(f"Cleaned email column: {email_column}")
        
        return df

    def process_csv_file(self, blob: storage.Blob, identifier: str) -> Optional[str]:
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            self.logger.info(f"Processing CSV file from SFTP: {blob.name} for {identifier} identifier")

            content = blob.download_as_bytes()
            df = self.read_csv_with_proper_dtypes(content)

            column_mappings = self.dag_config.get('column_mappings', {})
            if column_mappings:
                df.rename(columns=column_mappings, inplace=True)
                self.logger.info(f"Applied column mappings: {column_mappings}")

            raw_file_df_columns = df.columns.to_list()
            self.logger.info(f"CSV file columns: {raw_file_df_columns}")
            
            # Check for schema mismatch and filter valid rows
            schema_error_records, valid_df = self.check_schema_mismatch_and_filter(df, blob)
            if schema_error_records:
                self.logger.warning(f"Found {len(schema_error_records)} schema mismatch errors")
                self.load_error_records(schema_error_records, blob)
            
            # Continue processing with valid rows
            if valid_df.empty:
                self.logger.warning("No valid rows to process after schema validation")
                return None
            
            self.logger.info(f"Processing {len(valid_df)} valid rows")

            # Get phone and email column configurations
            phone_column = self.dag_config.get('phone_column')
            email_column = self.dag_config.get('email_column')
            
            # Check if we have either phone or email columns to process
            if (phone_column and phone_column in valid_df.columns) or (email_column and email_column in valid_df.columns):
                enriched_df, id_service_errors = self.enrich_with_mdlz_ids(valid_df, phone_column, email_column, blob.name)
                
                # Log the state before error processing
                successful_before_removal = len(enriched_df[enriched_df['mdlzID'].notna()])
                self.logger.info(f"Before error removal: {successful_before_removal} records with mdlzID")
                
                if id_service_errors:
                    self.logger.warning(f"Failed to fetch mdlzIDs for {len(id_service_errors)} records")
                    self.load_error_records(id_service_errors, blob)
                    
                    # FIXED: Improved error removal logic - only remove records that actually failed
                    error_row_indices = set()
                    
                    # Build a set of row indices that actually failed (have no mdlzID)
                    for index, row in enriched_df.iterrows():
                        if pd.isna(row.get('mdlzID')):
                            error_row_indices.add(index)
                    
                    if error_row_indices:
                        # Remove only the rows that actually failed (have no mdlzID)
                        initial_count = len(enriched_df)
                        enriched_df = enriched_df[enriched_df['mdlzID'].notna()]
                        final_count = len(enriched_df)
                        
                        self.logger.info(f"Removed {initial_count - final_count} rows without mdlzID. Remaining: {final_count}")
                        
                        # Validation: Check if removal count matches error count
                        removed_count = initial_count - final_count
                        if removed_count != len(id_service_errors):
                            self.logger.warning(f"MISMATCH: Removed {removed_count} rows but had {len(id_service_errors)} errors")
                            
                            # Debug: Log details about the mismatch
                            self.logger.debug(f"Error records phone/email identifiers:")
                            for i, error_record in enumerate(id_service_errors):
                                phone = error_record.get('phone')
                                email = error_record.get('email')
                                self.logger.debug(f"  Error {i+1}: phone={phone}, email={email}")
                    else:
                        self.logger.info("No rows to remove - all records have mdlzID")
            else:
                enriched_df = valid_df.copy()
                enriched_df['normalized_phone'] = None
                enriched_df['normalized_email'] = None
                enriched_df['mdlzID'] = None
            
            if enriched_df.empty:
                self.logger.warning("No valid rows remaining after ID service processing")
                return None
            
            # Final validation: Ensure all remaining records have mdlzID
            records_without_mdlzid = len(enriched_df[enriched_df['mdlzID'].isna()])
            if records_without_mdlzid > 0:
                self.logger.error(f"CRITICAL: {records_without_mdlzid} records without mdlzID found in final dataset!")
                # Remove any remaining records without mdlzID
                enriched_df = enriched_df[enriched_df['mdlzID'].notna()]
                self.logger.info(f"Removed {records_without_mdlzid} additional records without mdlzID")
            
            final_successful_count = len(enriched_df)
            self.logger.info(f"Final count for main table: {final_successful_count} records")
            
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


    def enrich_with_mdlz_ids(self, df: pd.DataFrame, phone_column: Optional[str], email_column: Optional[str], blob_name: str) -> Tuple[pd.DataFrame, List[Dict]]:
        """Enrich dataframe with Mondelez IDs using both phone and email with phone-first priority."""
        error_records = []
        
        if df.empty:
            self.logger.info("Empty dataframe provided for mdlzID enrichment")
            return df, error_records
        
        try:
            self.logger.info(f"Fetching mdlzIds for phone: {phone_column}, email: {email_column} in {blob_name}")
            
            # Initialize columns
            df['normalized_phone'] = None
            df['normalized_email'] = None
            df['mdlzID'] = None
            
            # CRITICAL FIX: Store original row data before any processing
            original_row_data = {}
            row_to_identifiers = {}  # Map row index to its identifiers
            phone_personas = []
            email_personas = []
            
            # First pass: collect all identifiers and store original data
            for index, row in df.iterrows():
                # Store complete original row data
                original_row_data[index] = {}
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        original_row_data[index][col] = None
                    else:
                        original_row_data[index][col] = value
                
                phone_value = None
                email_value = None
                original_phone = None
                original_email = None
                
                # Get and clean phone value
                if phone_column and phone_column in df.columns:
                    phone_raw = self.clean_phone_number(row.get(phone_column))
                    if phone_raw:
                        original_phone = phone_raw
                        # Normalize phone
                        if not phone_raw.startswith('+91'):
                            phone_value = f'+91{phone_raw}'
                        else:
                            phone_value = phone_raw
                        df.at[index, 'normalized_phone'] = phone_value
                
                # Get and clean email value
                if email_column and email_column in df.columns:
                    email_raw = self.clean_email_address(row.get(email_column))
                    if email_raw:
                        original_email = email_raw
                        email_value = email_raw
                        df.at[index, 'normalized_email'] = email_value
                
                # Store identifier mapping for this row with original data
                row_to_identifiers[index] = {
                    'phone': phone_value,
                    'email': email_value,
                    'original_phone': original_phone,
                    'original_email': original_email,
                    'has_phone': phone_value is not None,
                    'has_email': email_value is not None,
                    'original_data': original_row_data[index]  # Store original row data
                }
                
                # Collect personas for batch processing
                if phone_value:
                    phone_persona = PersonaFields(phone=phone_value, email=None)
                    phone_personas.append((phone_persona, index))
                
                if email_value:
                    email_persona = PersonaFields(phone=None, email=email_value)
                    email_personas.append((email_persona, index))
            
            # Step 1: Process all phone personas first
            phone_success = {}
            phone_errors = {}
            
            if phone_personas:
                self.logger.info(f"Step 1: Processing {len(phone_personas)} phone personas")
                phone_persona_list = [persona for persona, _ in phone_personas]
                phone_success, phone_errors = asyncio.run(fetch_mdlz_ids_parallel_personas(phone_persona_list))
                self.logger.info(f"Phone results: {len(phone_success)} successful, {len(phone_errors)} errors")
            
            # Step 2: Process all email personas
            email_success = {}
            email_errors = {}
            
            if email_personas:
                self.logger.info(f"Step 2: Processing {len(email_personas)} email personas")
                email_persona_list = [persona for persona, _ in email_personas]
                email_success, email_errors = asyncio.run(fetch_mdlz_ids_parallel_personas(email_persona_list))
                self.logger.info(f"Email results: {len(email_success)} successful, {len(email_errors)} errors")
            
            # Step 3: Apply phone-first priority logic for each row
            for index, row in df.iterrows():
                identifiers = row_to_identifiers[index]
                phone_value = identifiers['phone']
                email_value = identifiers['email']
                has_phone = identifiers['has_phone']
                has_email = identifiers['has_email']
                
                mdlz_id = None
                success_source = None
                
                # Phone-first priority logic
                if has_phone and phone_value in phone_success:
                    # Phone succeeded
                    mdlz_id = phone_success[phone_value]
                    success_source = 'phone'
                    self.logger.debug(f"Row {index}: Phone {phone_value} succeeded -> {mdlz_id}")
                    
                elif has_email and email_value in email_success:
                    # Email succeeded (either phone failed or no phone)
                    mdlz_id = email_success[email_value]
                    success_source = 'email'
                    if has_phone:
                        self.logger.debug(f"Row {index}: Phone failed, email {email_value} succeeded -> {mdlz_id}")
                    else:
                        self.logger.debug(f"Row {index}: Email-only {email_value} succeeded -> {mdlz_id}")
                
                # Set mdlzID if we found one
                if mdlz_id:
                    df.at[index, 'mdlzID'] = mdlz_id
                else:
                    # Both failed or no identifiers - create error record with original data
                    self.create_detailed_error_record_with_original_data(
                        index, identifiers, phone_success, phone_errors, 
                        email_success, email_errors, error_records
                    )
            
            # Count results
            successful_count = len(df[df['mdlzID'].notna()])
            error_count = len(error_records)
            total_count = len(df)
            
            self.logger.info(f"ID enrichment summary: {successful_count} successful, {error_count} errors out of {total_count} total records")
            
            # Validation check
            if successful_count + error_count != total_count:
                self.logger.error(f"DATA LOSS DETECTED! Input: {total_count}, Output: {successful_count + error_count}")
                raise Exception(f"Data loss detected: {total_count - (successful_count + error_count)} records missing")
            
            return df, error_records
            
        except Exception as e:
            self.logger.error(f"Error enriching with mdlz IDs: {e}")
            traceback.print_exc()
            
            # Create error records for all rows due to service failure
            for index, row in df.iterrows():
                self.create_service_failure_error_record(df, index, row_to_identifiers if 'row_to_identifiers' in locals() else {}, str(e), error_records)
            
            # Set default values for failed enrichment
            df['normalized_phone'] = None
            df['normalized_email'] = None
            df['mdlzID'] = None
            
            return df, error_records

    # def create_detailed_error_record(self, df: pd.DataFrame, row_index: int, identifiers: Dict, 
    #                                 phone_success: Dict, phone_errors: Dict, 
    #                                 email_success: Dict, email_errors: Dict, 
    #                                 error_records: List[Dict]) -> None:
    #     """Create detailed error record with proper failure analysis."""
    #     row = df.iloc[row_index]
        
    #     phone_value = identifiers['phone']
    #     email_value = identifiers['email']
    #     has_phone = identifiers['has_phone']
    #     has_email = identifiers['has_email']
    #     original_phone = identifiers['original_phone']
    #     original_email = identifiers['original_email']
        
    #     # Create sftpData with all raw data for this row
    #     sftp_data = {}
    #     for col in df.columns:
    #         if col in row:
    #             value = row[col]
    #             if pd.isna(value):
    #                 sftp_data[col] = None
    #             else:
    #                 sftp_data[col] = value
        
    #     # Determine failure reason
    #     error_detail = "mdlzID generation failed. "
        
    #     if has_phone and has_email:
    #         # Both phone and email available
    #         phone_status = "succeeded" if phone_value in phone_success else "failed"
    #         email_status = "succeeded" if email_value in email_success else "failed"
            
    #         if phone_status == "failed" and email_status == "failed":
    #             error_detail += f"Both phone {phone_value} and email {email_value} failed. "
                
    #             # Add phone error details
    #             if phone_value in phone_errors:
    #                 phone_error = phone_errors[phone_value]
    #                 error_detail += f"Phone error: {phone_error.get('details', 'Unknown error')}. "
                
    #             # Add email error details
    #             if email_value in email_errors:
    #                 email_error = email_errors[email_value]
    #                 error_detail += f"Email error: {email_error.get('details', 'Unknown error')}. "
    #         else:
    #             # This should not happen if logic is correct
    #             error_detail += f"Unexpected state: phone {phone_status}, email {email_status}. "
                
    #     elif has_phone:
    #         # Phone only
    #         if phone_value in phone_errors:
    #             phone_error = phone_errors[phone_value]
    #             error_detail += f"Phone {phone_value} failed: {phone_error.get('details', 'Unknown error')}. "
    #         else:
    #             error_detail += f"Phone {phone_value} failed: No response from ID service. "
                
    #     elif has_email:
    #         # Email only
    #         if email_value in email_errors:
    #             email_error = email_errors[email_value]
    #             error_detail += f"Email {email_value} failed: {email_error.get('details', 'Unknown error')}. "
    #         else:
    #             error_detail += f"Email {email_value} failed: No response from ID service. "
    #     else:
    #         # No identifiers
    #         error_detail += "No phone or email available for mdlzID lookup. "
        
    #     error_record = {
    #         'phone': original_phone,
    #         'email': original_email,
    #         'error_detail': error_detail.strip(),
    #         'data': sftp_data
    #     }
    #     error_records.append(error_record)
    #     self.logger.debug(f"Created error record for row {row_index}: {error_detail}")

    def create_detailed_error_record(self, df: pd.DataFrame, row_index: int, identifiers: Dict, 
                                    phone_success: Dict, phone_errors: Dict, 
                                    email_success: Dict, email_errors: Dict, 
                                    error_records: List[Dict]) -> None:
        """Create detailed error record with proper failure analysis."""
        row = df.iloc[row_index]
        
        phone_value = identifiers['phone']
        email_value = identifiers['email']
        has_phone = identifiers['has_phone']
        has_email = identifiers['has_email']
        original_phone = identifiers['original_phone']
        original_email = identifiers['original_email']
        
        # Create sftpData with all raw data for this row
        sftp_data = {}
        for col in df.columns:
            if col in row:
                value = row[col]
                if pd.isna(value):
                    sftp_data[col] = None
                else:
                    sftp_data[col] = value
        
        # Add channel level consent to error record data
        sftp_data_with_consent = self.add_channel_level_consent(
            sftp_data, original_phone, original_email
        )
        
        # Determine failure reason
        error_detail = "mdlzID generation failed. "
        
        if has_phone and has_email:
            # Both phone and email available
            phone_status = "succeeded" if phone_value in phone_success else "failed"
            email_status = "succeeded" if email_value in email_success else "failed"
            
            if phone_status == "failed" and email_status == "failed":
                error_detail += f"Both phone {phone_value} and email {email_value} failed. "
                
                # Add phone error details
                if phone_value in phone_errors:
                    phone_error = phone_errors[phone_value]
                    error_detail += f"Phone error: {phone_error.get('details', 'Unknown error')}. "
                
                # Add email error details
                if email_value in email_errors:
                    email_error = email_errors[email_value]
                    error_detail += f"Email error: {email_error.get('details', 'Unknown error')}. "
            else:
                # This should not happen if logic is correct
                error_detail += f"Unexpected state: phone {phone_status}, email {email_status}. "
                
        elif has_phone:
            # Phone only
            if phone_value in phone_errors:
                phone_error = phone_errors[phone_value]
                error_detail += f"Phone {phone_value} failed: {phone_error.get('details', 'Unknown error')}. "
            else:
                error_detail += f"Phone {phone_value} failed: No response from ID service. "
                
        elif has_email:
            # Email only
            if email_value in email_errors:
                email_error = email_errors[email_value]
                error_detail += f"Email {email_value} failed: {email_error.get('details', 'Unknown error')}. "
            else:
                error_detail += f"Email {email_value} failed: No response from ID service. "
        else:
            # No identifiers
            error_detail += "No phone or email available for mdlzID lookup. "
        
        error_record = {
            'phone': original_phone,
            'email': original_email,
            'error_detail': error_detail.strip(),
            'data': sftp_data_with_consent
        }
        error_records.append(error_record)
        self.logger.debug(f"Created error record for row {row_index}: {error_detail}")

    # def create_service_failure_error_record(self, df: pd.DataFrame, row_index: int, 
    #                                     row_to_identifiers: Dict, error_msg: str, 
    #                                     error_records: List[Dict]) -> None:
    #     """Create error record for service failures."""
    #     row = df.iloc[row_index]
        
    #     # Create sftpData with all raw data for this row
    #     sftp_data = {}
    #     for col in df.columns:
    #         if col in row:
    #             value = row[col]
    #             if pd.isna(value):
    #                 sftp_data[col] = None
    #             else:
    #                 sftp_data[col] = value
        
    #     # Get original identifiers if available
    #     identifiers = row_to_identifiers.get(row_index, {})
    #     original_phone = identifiers.get('original_phone')
    #     original_email = identifiers.get('original_email')
        
    #     error_record = {
    #         'phone': original_phone,
    #         'email': original_email,
    #         'error_detail': f"ID service error: {error_msg}",
    #         'data': sftp_data
    #     }
    #     error_records.append(error_record)
    
    def create_service_failure_error_record(self, df: pd.DataFrame, row_index: int, 
                                        row_to_identifiers: Dict, error_msg: str, 
                                        error_records: List[Dict]) -> None:
        """Create error record for service failures."""
        row = df.iloc[row_index]
        
        # Create sftpData with all raw data for this row
        sftp_data = {}
        for col in df.columns:
            if col in row:
                value = row[col]
                if pd.isna(value):
                    sftp_data[col] = None
                else:
                    sftp_data[col] = value
        
        # Get original identifiers if available
        identifiers = row_to_identifiers.get(row_index, {})
        original_phone = identifiers.get('original_phone')
        original_email = identifiers.get('original_email')
        
        # Add channel level consent to error record data
        sftp_data_with_consent = self.add_channel_level_consent(
            sftp_data, original_phone, original_email
        )
        
        error_record = {
            'phone': original_phone,
            'email': original_email,
            'error_detail': f"ID service error: {error_msg}",
            'data': sftp_data_with_consent
        }
        error_records.append(error_record)

    # def create_detailed_error_record_with_original_data(self, row_index: int, identifiers: Dict, 
    #                                                phone_success: Dict, phone_errors: Dict, 
    #                                                email_success: Dict, email_errors: Dict, 
    #                                                error_records: List[Dict]) -> None:
    #     """Create detailed error record using original stored data to prevent data mixing."""
        
    #     phone_value = identifiers['phone']
    #     email_value = identifiers['email']
    #     has_phone = identifiers['has_phone']
    #     has_email = identifiers['has_email']
    #     original_phone = identifiers['original_phone']
    #     original_email = identifiers['original_email']
        
    #     # Use the original stored data instead of current dataframe row
    #     sftp_data = identifiers['original_data'].copy()
        
    #     # Determine failure reason
    #     error_detail = "mdlzID generation failed. "
        
    #     if has_phone and has_email:
    #         # Both phone and email available
    #         phone_status = "succeeded" if phone_value in phone_success else "failed"
    #         email_status = "succeeded" if email_value in email_success else "failed"
            
    #         if phone_status == "failed" and email_status == "failed":
    #             error_detail += f"Both phone {phone_value} and email {email_value} failed. "
                
    #             # Add phone error details
    #             if phone_value in phone_errors:
    #                 phone_error = phone_errors[phone_value]
    #                 error_detail += f"Phone error: {phone_error.get('details', 'Unknown error')}. "
                
    #             # Add email error details
    #             if email_value in email_errors:
    #                 email_error = email_errors[email_value]
    #                 error_detail += f"Email error: {email_error.get('details', 'Unknown error')}. "
    #         else:
    #             # This should not happen if logic is correct
    #             error_detail += f"Unexpected state: phone {phone_status}, email {email_status}. "
                
    #     elif has_phone:
    #         # Phone only
    #         if phone_value in phone_errors:
    #             phone_error = phone_errors[phone_value]
    #             error_detail += f"Phone {phone_value} failed: {phone_error.get('details', 'Unknown error')}. "
    #         else:
    #             error_detail += f"Phone {phone_value} failed: No response from ID service. "
                
    #     elif has_email:
    #         # Email only
    #         if email_value in email_errors:
    #             email_error = email_errors[email_value]
    #             error_detail += f"Email {email_value} failed: {email_error.get('details', 'Unknown error')}. "
    #         else:
    #             error_detail += f"Email {email_value} failed: No response from ID service. "
    #     else:
    #         # No identifiers
    #         error_detail += "No phone or email available for mdlzID lookup. "
        
    #     error_record = {
    #         'phone': original_phone,
    #         'email': original_email,
    #         'error_detail': error_detail.strip(),
    #         'data': sftp_data
    #     }
    #     error_records.append(error_record)
        
    #     # Enhanced logging for debugging
    #     self.logger.debug(f"Created error record for row {row_index}: phone={original_phone}, email={original_email}")
    #     self.logger.debug(f"Original data phone: {sftp_data.get(self.dag_config.get('phone_column'))}")
    #     self.logger.debug(f"Original data email: {sftp_data.get(self.dag_config.get('email_column'))}")

    def create_detailed_error_record_with_original_data(self, row_index: int, identifiers: Dict, 
                                                   phone_success: Dict, phone_errors: Dict, 
                                                   email_success: Dict, email_errors: Dict, 
                                                   error_records: List[Dict]) -> None:
        """Create detailed error record using original stored data to prevent data mixing."""
        
        phone_value = identifiers['phone']
        email_value = identifiers['email']
        has_phone = identifiers['has_phone']
        has_email = identifiers['has_email']
        original_phone = identifiers['original_phone']
        original_email = identifiers['original_email']
        
        # Use the original stored data instead of current dataframe row
        sftp_data = identifiers['original_data'].copy()
        
        # Add channel level consent to error record data
        sftp_data_with_consent = self.add_channel_level_consent(
            sftp_data, original_phone, original_email
        )
        
        # Determine failure reason
        error_detail = "mdlzID generation failed. "
        
        if has_phone and has_email:
            # Both phone and email available
            phone_status = "succeeded" if phone_value in phone_success else "failed"
            email_status = "succeeded" if email_value in email_success else "failed"
            
            if phone_status == "failed" and email_status == "failed":
                error_detail += f"Both phone {phone_value} and email {email_value} failed. "
                
                # Add phone error details
                if phone_value in phone_errors:
                    phone_error = phone_errors[phone_value]
                    error_detail += f"Phone error: {phone_error.get('details', 'Unknown error')}. "
                
                # Add email error details
                if email_value in email_errors:
                    email_error = email_errors[email_value]
                    error_detail += f"Email error: {email_error.get('details', 'Unknown error')}. "
            else:
                # This should not happen if logic is correct
                error_detail += f"Unexpected state: phone {phone_status}, email {email_status}. "
                
        elif has_phone:
            # Phone only
            if phone_value in phone_errors:
                phone_error = phone_errors[phone_value]
                error_detail += f"Phone {phone_value} failed: {phone_error.get('details', 'Unknown error')}. "
            else:
                error_detail += f"Phone {phone_value} failed: No response from ID service. "
                
        elif has_email:
            # Email only
            if email_value in email_errors:
                email_error = email_errors[email_value]
                error_detail += f"Email {email_value} failed: {email_error.get('details', 'Unknown error')}. "
            else:
                error_detail += f"Email {email_value} failed: No response from ID service. "
        else:
            # No identifiers
            error_detail += "No phone or email available for mdlzID lookup. "
        
        error_record = {
            'phone': original_phone,
            'email': original_email,
            'error_detail': error_detail.strip(),
            'data': sftp_data_with_consent
        }
        error_records.append(error_record)
        
        # Enhanced logging for debugging
        self.logger.debug(f"Created error record for row {row_index}: phone={original_phone}, email={original_email}")
        self.logger.debug(f"Original data phone: {sftp_data_with_consent.get(self.dag_config.get('phone_column'))}")
        self.logger.debug(f"Original data email: {sftp_data_with_consent.get(self.dag_config.get('email_column'))}")