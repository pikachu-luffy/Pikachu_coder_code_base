import os
import json
import asyncio
import pendulum
import tempfile
import traceback
import pandas as pd
from helpers.config import Config
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage
from typing import Dict, List, Optional, Tuple, Any, Set
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


    # def identify_received_columns(self, df: pd.DataFrame) -> Tuple[Set[str], Set[str], Dict[str, str]]:
    #     """
    #     Identify which columns from column_mappings were actually received from vendor.
        
    #     Args:
    #         df: DataFrame with actual file columns
            
    #     Returns:
    #         Tuple of:
    #         - received_mapped_columns: Set of target column names that were received
    #         - missing_mapped_columns: Set of target column names that were missing
    #         - received_column_mapping: Dict mapping only the received columns
    #     """
    #     column_mappings = self.dag_config.get('column_mappings', {})
    #     file_columns = set(df.columns.tolist())
        
    #     # Find which source columns from mappings are actually present in the file
    #     received_source_columns = set()
    #     missing_source_columns = set()
        
    #     for source_col, target_col in column_mappings.items():
    #         if source_col in file_columns:
    #             received_source_columns.add(source_col)
    #         else:
    #             missing_source_columns.add(source_col)
        
    #     # Create mappings for received and missing columns
    #     received_mapped_columns = {column_mappings[col] for col in received_source_columns}
    #     missing_mapped_columns = {column_mappings[col] for col in missing_source_columns}
    #     received_column_mapping = {source: target for source, target in column_mappings.items() if source in received_source_columns}
        
    #     self.logger.info(f"Column analysis:")
    #     self.logger.info(f"  - Total expected columns in mappings: {len(column_mappings)}")
    #     self.logger.info(f"  - Received columns from vendor: {len(received_source_columns)} -> {list(received_source_columns)}")
    #     self.logger.info(f"  - Missing columns from vendor: {len(missing_source_columns)} -> {list(missing_source_columns)}")
    #     self.logger.info(f"  - Received mapped columns: {list(received_mapped_columns)}")
    #     self.logger.info(f"  - Missing mapped columns: {list(missing_mapped_columns)}")
        
    #     return received_mapped_columns, missing_mapped_columns, received_column_mapping
    

    # --- UPDATED: identify_received_columns ---
    def identify_received_columns(self, df: pd.DataFrame) -> Tuple[Set[str], Set[str], Dict[str, str]]:
        """
        Identify which columns from column_mappings were actually received from vendor.
        
        Args:
            df: DataFrame with actual file columns
            
        Returns:
            Tuple of:
            - received_mapped_columns: Set of target column names that were received
            - missing_mapped_columns: Set of target column names that were missing (excluding any target already received)
            - received_column_mapping: Dict mapping only the received columns
        """
        column_mappings = self.dag_config.get('column_mappings', {})
        file_columns = set(df.columns.tolist())
        
        # Find which source columns from mappings are actually present in the file
        received_source_columns = set()
        missing_source_columns = set()
        
        for source_col, target_col in column_mappings.items():
            if source_col in file_columns:
                received_source_columns.add(source_col)
            else:
                missing_source_columns.add(source_col)
        
        # Create mappings for received and missing columns
        received_mapped_columns: Set[str] = {column_mappings[col] for col in received_source_columns}
        # IMPORTANT: Exclude any target that's already in received_mapped_columns to avoid later overwrites
        missing_mapped_columns: Set[str] = {
            column_mappings[col]
            for col in missing_source_columns
            if column_mappings[col] not in received_mapped_columns
        }
        received_column_mapping = {source: target for source, target in column_mappings.items() if source in received_source_columns}
        
        self.logger.info(f"Column analysis:")
        self.logger.info(f"  - Total expected columns in mappings: {len(column_mappings)}")
        self.logger.info(f"  - Received columns from vendor: {len(received_source_columns)} -> {list(received_source_columns)}")
        self.logger.info(f"  - Missing columns from vendor: {len(missing_source_columns)} -> {list(missing_source_columns)}")
        self.logger.info(f"  - Received mapped columns: {list(received_mapped_columns)}")
        self.logger.info(f"  - Missing mapped columns (filtered): {list(missing_mapped_columns)}")
        
        return received_mapped_columns, missing_mapped_columns, received_column_mapping
    
    def add_channel_level_consent(self, data: Dict, phone_value: Optional[str], email_value: Optional[str], consent_timestamp: Optional[str] = None) -> Dict:
        """
        Add channel level consent fields based on phone/email availability.
        
        Args:
            data: Dictionary containing the row data
            phone_value: Cleaned phone number (original format)
            email_value: Cleaned email address
            consent_timestamp: Timestamp from consent_timestamp column (can be any format)
            
        Returns:
            Dictionary with added consent fields
        """
        # Create a copy to avoid modifying original data
        enriched_data = data.copy()
        
        # Get consent timestamp from the data if not provided
        if not consent_timestamp:
            consent_timestamp = data.get('consent_timestamp')
        
        # Convert timestamp to desired format: "YYYY-MM-DD HH:MM:SS UTC"
        formatted_timestamp = None
        if consent_timestamp is not None:
            try:
                formatted_timestamp = self.parse_timestamp_to_utc_format(consent_timestamp)
            except Exception as e:
                self.logger.warning(f"Error parsing consent timestamp '{consent_timestamp}': {e}")
                # Fallback: use original value as string if parsing fails
                formatted_timestamp = str(consent_timestamp) if consent_timestamp else None
        
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

    # def parse_timestamp_to_utc_format(self, timestamp_value) -> Optional[str]:
    #     """
    #     Parse various timestamp formats and return in 'YYYY-MM-DD HH:MM:SS UTC' format.
        
    #     Handles:
    #     - Excel serial date numbers (e.g., 44896.3515625)
    #     - Unix timestamps (seconds since epoch)
    #     - ISO format strings (e.g., '2025-09-04T08:26:15Z')
    #     - UTC format strings (e.g., '2025-09-04 08:26:15 UTC')
    #     - Other common date formats
        
    #     Args:
    #         timestamp_value: Timestamp in any supported format
            
    #     Returns:
    #         Formatted timestamp string in 'YYYY-MM-DD HH:MM:SS UTC' format or None
    #     """
    #     if timestamp_value is None:
    #         return None
        
    #     try:
    #         # Handle numeric values (Excel serial dates or Unix timestamps)
    #         if isinstance(timestamp_value, (int, float)):
    #             numeric_value = float(timestamp_value)
                
    #             # Check if it's an Excel serial date (typically between 1 and 100000)
    #             if 1 <= numeric_value <= 100000:
    #                 # Excel serial date: days since 1900-01-01 (with Excel's leap year bug)
    #                 # Excel incorrectly treats 1900 as a leap year, so we need to account for that
    #                 excel_epoch = datetime(1899, 12, 30)  # Excel's epoch (accounting for the bug)
    #                 dt = excel_epoch + timedelta(days=numeric_value)
    #                 return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
    #             # Check if it's a Unix timestamp (seconds since 1970-01-01)
    #             elif numeric_value > 1000000000:  # Reasonable Unix timestamp range
    #                 dt = datetime.fromtimestamp(numeric_value, tz=timezone.utc)
    #                 return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
    #             # Check if it's a Unix timestamp in milliseconds
    #             elif numeric_value > 1000000000000:
    #                 dt = datetime.fromtimestamp(numeric_value / 1000, tz=timezone.utc)
    #                 return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
    #             else:
    #                 # Treat as days since epoch as fallback
    #                 dt = datetime(1970, 1, 1) + timedelta(days=numeric_value)
    #                 return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            
    #         # Handle string values
    #         elif isinstance(timestamp_value, str):
    #             timestamp_str = timestamp_value.strip()
                
    #             if not timestamp_str:
    #                 return None
                
    #             # Try to parse as numeric string first
    #             try:
    #                 numeric_value = float(timestamp_str)
    #                 return self._parse_timestamp_to_utc_format(numeric_value)
    #             except ValueError:
    #                 pass
                
    #             # Handle already formatted UTC strings
    #             if timestamp_str.endswith(' UTC'):
    #                 # Validate and reformat if needed
    #                 try:
    #                     dt_part = timestamp_str.replace(' UTC', '')
    #                     dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')
    #                     return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    #                 except ValueError:
    #                     pass
                
    #             # Handle ISO format strings
    #             if 'T' in timestamp_str:
    #                 try:
    #                     # Remove timezone info and parse
    #                     clean_str = timestamp_str.replace('Z', '').replace('+00:00', '')
    #                     if '.' in clean_str:
    #                         # Handle microseconds
    #                         dt = datetime.fromisoformat(clean_str)
    #                     else:
    #                         dt = datetime.fromisoformat(clean_str)
    #                     return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    #                 except ValueError:
    #                     pass
                
    #             # Try common date formats
    #             common_formats = [
    #                 '%Y-%m-%d %H:%M:%S',
    #                 '%Y-%m-%d %H:%M',
    #                 '%Y-%m-%d',
    #                 '%d/%m/%Y %H:%M:%S',
    #                 '%d/%m/%Y %H:%M',
    #                 '%d/%m/%Y',
    #                 '%m/%d/%Y %H:%M:%S',
    #                 '%m/%d/%Y %H:%M',
    #                 '%m/%d/%Y',
    #                 '%d-%m-%Y %H:%M:%S',
    #                 '%d-%m-%Y %H:%M',
    #                 '%d-%m-%Y',
    #                 '%Y/%m/%d %H:%M:%S',
    #                 '%Y/%m/%d %H:%M',
    #                 '%Y/%m/%d'
    #             ]
                
    #             for fmt in common_formats:
    #                 try:
    #                     dt = datetime.strptime(timestamp_str, fmt)
    #                     return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    #                 except ValueError:
    #                     continue
                
    #             # If all parsing attempts fail, log and return original
    #             self.logger.warning(f"Could not parse timestamp format: '{timestamp_str}'")
    #             return timestamp_str
            
    #         else:
    #             # Handle other types (convert to string and retry)
    #             return self._parse_timestamp_to_utc_format(str(timestamp_value))
        
    #     except Exception as e:
    #         self.logger.error(f"Error in timestamp parsing: {e}")
    #         return str(timestamp_value) if timestamp_value else None
    

    def parse_timestamp_to_utc_format(self, timestamp_value) -> Optional[str]:
        """
        Parse various timestamp formats and return in 'YYYY-MM-DD HH:MM:SS UTC' format.
        
        Handles:
        - Excel serial date numbers (e.g., 44896.3515625)
        - Unix timestamps (seconds since epoch)
        - ISO format strings (e.g., '2025-09-04T08:26:15Z')
        - UTC format strings (e.g., '2025-09-04 08:26:15 UTC')
        - Other common date formats
        
        Args:
            timestamp_value: Timestamp in any supported format
            
        Returns:
            Formatted timestamp string in 'YYYY-MM-DD HH:MM:SS UTC' format or None
        """
        if timestamp_value is None:
            return None
        
        try:
            # Handle numeric values (Excel serial dates or Unix timestamps)
            if isinstance(timestamp_value, (int, float)):
                numeric_value = float(timestamp_value)
                
                # Check if it's an Excel serial date (typically between 1 and 100000)
                if 1 <= numeric_value <= 100000:
                    # Excel serial date: days since 1900-01-01 (with Excel's leap year bug)
                    # Excel incorrectly treats 1900 as a leap year, so we need to account for that
                    excel_epoch = datetime(1899, 12, 30)  # Excel's epoch (accounting for the bug)
                    dt = excel_epoch + timedelta(days=numeric_value)
                    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
                # Check if it's a Unix timestamp (seconds since 1970-01-01)
                elif numeric_value > 1000000000:  # Reasonable Unix timestamp range
                    dt = datetime.fromtimestamp(numeric_value, tz=timezone.utc)
                    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
                # Check if it's a Unix timestamp in milliseconds
                elif numeric_value > 1000000000000:
                    dt = datetime.fromtimestamp(numeric_value / 1000, tz=timezone.utc)
                    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                
                else:
                    # Treat as days since epoch as fallback
                    dt = datetime(1970, 1, 1) + timedelta(days=numeric_value)
                    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            
            # Handle string values
            elif isinstance(timestamp_value, str):
                timestamp_str = timestamp_value.strip()
                
                if not timestamp_str:
                    return None
                
                # Try to parse as numeric string first
                try:
                    numeric_value = float(timestamp_str)
                    return self.parse_timestamp_to_utc_format(numeric_value)  # FIXED: was self._parse_timestamp_to_utc_format
                except ValueError:
                    pass
                
                # Handle already formatted UTC strings
                if timestamp_str.endswith(' UTC'):
                    # Validate and reformat if needed
                    try:
                        dt_part = timestamp_str.replace(' UTC', '')
                        dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')
                        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except ValueError:
                        pass
                
                # Handle ISO format strings
                if 'T' in timestamp_str:
                    try:
                        # Remove timezone info and parse
                        clean_str = timestamp_str.replace('Z', '').replace('+00:00', '')
                        if '.' in clean_str:
                            # Handle microseconds
                            dt = datetime.fromisoformat(clean_str)
                        else:
                            dt = datetime.fromisoformat(clean_str)
                        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except ValueError:
                        pass
                
                # Try common date formats
                common_formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%Y-%m-%d',
                    '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y %H:%M',
                    '%d/%m/%Y',
                    '%m/%d/%Y %H:%M:%S',
                    '%m/%d/%Y %H:%M',
                    '%m/%d/%Y',
                    '%d-%m-%Y %H:%M:%S',
                    '%d-%m-%Y %H:%M',
                    '%d-%m-%Y',
                    '%Y/%m/%d %H:%M:%S',
                    '%Y/%m/%d %H:%M',
                    '%Y/%m/%d'
                ]
                
                for fmt in common_formats:
                    try:
                        dt = datetime.strptime(timestamp_str, fmt)
                        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                    except ValueError:
                        continue
                
                # If all parsing attempts fail, log and return original
                self.logger.warning(f"Could not parse timestamp format: '{timestamp_str}'")
                return timestamp_str
            
            else:
                # Handle other types (convert to string and retry)
                return self.parse_timestamp_to_utc_format(str(timestamp_value))  # FIXED: was self._parse_timestamp_to_utc_format
        
        except Exception as e:
            self.logger.error(f"Error in timestamp parsing: {e}")
            return str(timestamp_value) if timestamp_value else None
    
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
        
        # DEBUG: Track all rows being processed
        total_input_rows = len(df)
        self.logger.info(f"DEBUG: Starting schema validation for {total_input_rows} rows")
        
        # Check each row for unexpected column data
        valid_row_indices = []
        schema_error_rows = []  # Store rows with schema mismatch for ID service processing
        phone_column = self.dag_config.get('phone_column', 'phone')
        email_column = self.dag_config.get('email_column', 'email')
        
        # DEBUG: Track processed rows
        processed_row_count = 0
        
        for index, row in df.iterrows():
            processed_row_count += 1
            self.logger.debug(f"DEBUG: Processing row {index} (count: {processed_row_count})")
            
            # Check if this row has non-null values in unexpected columns
            has_unexpected_data = False
            unexpected_data_columns = []
            
            try:
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
                    self.logger.debug(f"Row {index} marked as valid - no unexpected data")
                    
            except Exception as e:
                self.logger.error(f"DEBUG: Error processing row {index}: {e}")
                self.logger.error(f"DEBUG: Row data: {dict(row)}")
                # Don't add to either list - this might be the missing row
                continue
        
        # DEBUG: Verify row counts
        total_processed = len(valid_row_indices) + len(schema_error_rows)
        self.logger.info(f"DEBUG: Row processing summary:")
        self.logger.info(f"DEBUG: - Input rows: {total_input_rows}")
        self.logger.info(f"DEBUG: - Processed rows: {processed_row_count}")
        self.logger.info(f"DEBUG: - Valid rows: {len(valid_row_indices)}")
        self.logger.info(f"DEBUG: - Schema error rows: {len(schema_error_rows)}")
        self.logger.info(f"DEBUG: - Total accounted: {total_processed}")
        
        if total_processed != total_input_rows:
            self.logger.error(f"DEBUG: MISSING ROWS DETECTED! {total_input_rows - total_processed} rows unaccounted for")
            
            # Find missing row indices
            all_processed_indices = set(valid_row_indices + [row['index'] for row in schema_error_rows])
            all_input_indices = set(df.index)
            missing_indices = all_input_indices - all_processed_indices
            
            if missing_indices:
                self.logger.error(f"DEBUG: Missing row indices: {missing_indices}")
                for missing_idx in missing_indices:
                    try:
                        missing_row = df.loc[missing_idx]
                        self.logger.error(f"DEBUG: Missing row {missing_idx} data: {dict(missing_row)}")
                        
                        # Check what happened to this row
                        has_unexpected_data_debug = False
                        for col in unexpected_columns:
                            if col in missing_row:
                                value = missing_row[col]
                                is_na = pd.isna(value)
                                str_value = str(value).strip() if not is_na else "NaN"
                                self.logger.error(f"DEBUG: Missing row {missing_idx}, col '{col}': value='{value}', isna={is_na}, str_stripped='{str_value}'")
                                
                                if not is_na and str_value != '':
                                    has_unexpected_data_debug = True
                        
                        self.logger.error(f"DEBUG: Missing row {missing_idx} should have unexpected_data={has_unexpected_data_debug}")
                        
                    except Exception as e:
                        self.logger.error(f"DEBUG: Error analyzing missing row {missing_idx}: {e}")
        
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
                
                # FIXED: Handle 3 return values from fetch_mdlz_ids_parallel_personas
                if personas_for_id_service:
                    id_success, id_errors, normalized_lookup = asyncio.run(fetch_mdlz_ids_parallel_personas(personas_for_id_service))
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
                        
                        # ENHANCED: Add normalized values to the error record data using normalized_lookup
                        if error_row['phone'] and error_row['phone'] in normalized_lookup:
                            row_data_with_consent['normalized_phone'] = normalized_lookup[error_row['phone']]
                        if error_row['email'] and error_row['email'] in normalized_lookup:
                            row_data_with_consent['normalized_email'] = normalized_lookup[error_row['email']]
                        
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
    
    # def create_complete_sftp_data_with_selective_population(self, row: pd.Series, received_mapped_columns: Set[str], missing_mapped_columns: Set[str]) -> Dict:
    #     """
    #     Create complete sftpData with selective population based on received columns.
        
    #     Args:
    #         row: DataFrame row with actual data
    #         received_mapped_columns: Set of target column names that were received from vendor
    #         missing_mapped_columns: Set of target column names that were missing from vendor
            
    #     Returns:
    #         Dictionary with all expected columns, populated selectively
    #     """
    #     sftp_data = {}
        
    #     # First, populate received columns with actual values
    #     for col in received_mapped_columns:
    #         if col in row.index:
    #             value = row[col]
    #             if pd.isna(value):
    #                 sftp_data[col] = None
    #             else:
    #                 sftp_data[col] = value
    #         else:
    #             # Column was expected but not found in row (shouldn't happen after mapping)
    #             sftp_data[col] = None
        
    #     # Then, add missing columns as NULL
    #     for col in missing_mapped_columns:
    #         sftp_data[col] = None
        
    #     # Add system-generated columns that are always present
    #     system_columns = ['mdlz_email_consent_status', 'mdlz_email_consent_status_ts', 
    #                      'mdlz_sms_consent_status', 'mdlz_sms_consent_status_ts', 
    #                      'mdlz_whatsapp_consent_status', 'mdlz_whatsapp_consent_status_ts']
        
    #     for col in system_columns:
    #         if col in row.index:
    #             value = row[col]
    #             if pd.isna(value):
    #                 sftp_data[col] = None
    #             else:
    #                 sftp_data[col] = value
    #         else:
    #             sftp_data[col] = None
        
    #     return sftp_data

    # def create_complete_sftp_data_with_selective_population(self, row: pd.Series, received_mapped_columns: Set[str], missing_mapped_columns: Set[str]) -> Dict:
    #     """
    #     Create complete sftpData with selective population based on received columns.
        
    #     Args:
    #         row: DataFrame row with actual data
    #         received_mapped_columns: Set of target column names that were received from vendor
    #         missing_mapped_columns: Set of target column names that were missing from vendor
            
    #     Returns:
    #         Dictionary with all expected columns, populated selectively
    #     """
    #     sftp_data = {}
        
    #     # First, populate ALL columns that exist in the row (this preserves existing logic)
    #     for col in row.index:
    #         if col != 'mdlzID':  # Exclude mdlzID from sftpData
    #             value = row[col]
    #             if pd.isna(value):
    #                 sftp_data[col] = None
    #             else:
    #                 sftp_data[col] = value
        
    #     # Then, add missing columns from column_mappings as NULL (this is the NEW selective logic)
    #     for col in missing_mapped_columns:
    #         if col not in sftp_data:  # Only add if not already present
    #             sftp_data[col] = None
        
    #     return sftp_data


    def create_complete_sftp_data_with_selective_population(
        self,
        row: pd.Series,
        received_mapped_columns: Set[str],
        missing_mapped_columns: Set[str]
    ) -> Dict:
        """
        Create complete sftpData with selective population based on received columns.

        Args:
            row: DataFrame row with actual data (may have duplicate column labels)
            received_mapped_columns: Set of target column names that were received from vendor
            missing_mapped_columns: Set of target column names that were missing from vendor

        Returns:
            Dictionary with all expected columns, populated selectively
        """
        sftp_data: Dict[str, Any] = {}

        # IMPORTANT: iterate over items to avoid label-based lookup that can return a Series
        # when duplicate column names exist. This yields scalar values per position.
        for col, value in row.items():
            if col == "mdlzID":  # Exclude mdlzID from sftpData
                continue

            # Preserve existing logic: store None for NaNs
            sftp_data[col] = None if pd.isna(value) else value

        # Add missing mapped columns as NULL without overwriting already-populated keys
        for col in missing_mapped_columns:
            if col not in sftp_data:
                sftp_data[col] = None

        return sftp_data

    # def transform_to_json_format(self, df: pd.DataFrame, blob: storage.Blob) -> pd.DataFrame:
    #     """Transform dataframe to have only mdlzID and sftpData (JSON) columns with channel level consent."""
    #     try:
    #         self.logger.info("Transforming dataframe to JSON format with mdlzID and sftpData columns")
            
    #         # Get phone and email column names
    #         phone_column = self.dag_config.get('phone_column', 'phone')
    #         email_column = self.dag_config.get('email_column', 'email')
            
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
                
    #             # Get phone and email values for consent logic
    #             phone_value = self.clean_phone_number(row.get(phone_column))
    #             email_value = self.clean_email_address(row.get(email_column))
                
    #             # Add channel level consent fields
    #             sftp_data_with_consent = self.add_channel_level_consent(
    #                 sftp_data, phone_value, email_value
    #             )
                
    #             # Convert sftpData to JSON string
    #             sftp_data_json = json.dumps(sftp_data_with_consent, default=str)
                
    #             transformed_data.append({
    #                 'mdlzID': mdlz_id,
    #                 'sftpData': sftp_data_json,
    #                 'filename': blob.name,
    #             })
            
    #         transformed_df = pd.DataFrame(transformed_data)
    #         self.logger.info(f"Successfully transformed {len(transformed_df)} records to JSON format with channel level consent")
            
    #         return transformed_df
    #     except Exception as e:
    #         self.logger.error(f"Error transforming dataframe to JSON format: {e}")
    #         raise e
    
    def transform_to_json_format(self, df: pd.DataFrame, blob: storage.Blob) -> pd.DataFrame:
        """Transform dataframe to have only mdlzID and sftpData (JSON) columns with selective column population."""
        try:
            self.logger.info("Transforming dataframe to JSON format with selective column population")
            
            # Get phone and email column names
            phone_column = self.dag_config.get('phone_column', 'phone')
            email_column = self.dag_config.get('email_column', 'email')
            
            # Identify which columns were actually received vs missing
            received_mapped_columns, missing_mapped_columns, received_column_mapping = self.identify_received_columns(df)
            
            # Create a new dataframe with only mdlzID and sftpData columns
            transformed_data = []
            
            for index, row in df.iterrows():
                # Extract mdlzID
                mdlz_id = row.get('mdlzID', None)
                
                # Create sftpData JSON object with selective population
                sftp_data = self.create_complete_sftp_data_with_selective_population(
                    row, received_mapped_columns, missing_mapped_columns
                )
                
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
                    'sftpData': sftp_data_json,
                    'filename': blob.name,
                })
            
            transformed_df = pd.DataFrame(transformed_data)
            
            # Log selective population statistics
            total_expected_columns = len(received_mapped_columns) + len(missing_mapped_columns)
            self.logger.info(f"Selective column population summary:")
            self.logger.info(f"  - Total expected columns from mappings: {total_expected_columns}")
            self.logger.info(f"  - Columns received from vendor: {len(received_mapped_columns)} -> {list(received_mapped_columns)}")
            self.logger.info(f"  - Columns set to NULL (missing): {len(missing_mapped_columns)} -> {list(missing_mapped_columns)}")
            self.logger.info(f"Successfully transformed {len(transformed_df)} records to JSON format with selective population")
            
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

    def has_hashed_identifiers(self, row: pd.Series) -> bool:
        """
        Check if a row has phone_hashed or email_hashed values.
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row has phone_hashed or email_hashed, False otherwise
        """
        phone_hashed_column = self.dag_config.get('phone_hashed_column', 'phone_hashed')
        email_hashed_column = self.dag_config.get('email_hashed_column', 'email_hashed')
        
        # Check if phone_hashed exists and has value
        has_phone_hashed = False
        if phone_hashed_column in row:
            phone_hashed_value = row[phone_hashed_column]
            if not pd.isna(phone_hashed_value) and str(phone_hashed_value).strip() != '':
                has_phone_hashed = True
        
        # Check if email_hashed exists and has value
        has_email_hashed = False
        if email_hashed_column in row:
            email_hashed_value = row[email_hashed_column]
            if not pd.isna(email_hashed_value) and str(email_hashed_value).strip() != '':
                has_email_hashed = True
        
        return has_phone_hashed or has_email_hashed

    def read_csv_with_proper_dtypes(self, content: bytes) -> pd.DataFrame:
        """Read CSV with proper data types to prevent phone number float conversion."""
        phone_column = self.dag_config.get('phone_column', 'phone')
        email_column = self.dag_config.get('email_column', 'email')
        phone_hashed_column = self.dag_config.get('phone_hashed_column', 'phone_hashed')
        email_hashed_column = self.dag_config.get('email_hashed_column', 'email_hashed')
        
        # Create dtype dictionary to force string types for phone/email columns
        dtype_dict = {}
        
        # First, read just the header to get column names
        header_df = pd.read_csv(pd.io.common.BytesIO(content), nrows=0)
        
        # Set string dtype for phone, email, and hashed columns that exist in the file
        for col in header_df.columns:
            if col in [phone_column, email_column, phone_hashed_column, email_hashed_column]:
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

    # def process_csv_file(self, blob: storage.Blob, identifier: str) -> Optional[str]:
    #     try:
    #         pipeline_load_start = datetime.now(pendulum.UTC)
    #         record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
    #         self.logger.info(f"Processing CSV file from SFTP: {blob.name} for {identifier} identifier @ {record_created_at}")

    #         content = blob.download_as_bytes()
    #         df = self.read_csv_with_proper_dtypes(content)

    #         column_mappings = self.dag_config.get('column_mappings', {})
    #         if column_mappings:
    #             df.rename(columns=column_mappings, inplace=True)
    #             self.logger.info(f"Applied selective column mappings for received columns: {column_mappings}")

    #         raw_file_df_columns = df.columns.to_list()
    #         self.logger.info(f"CSV file columns: {raw_file_df_columns}")
            
    #         # Check for schema mismatch and filter valid rows
    #         schema_error_records, valid_df = self.check_schema_mismatch_and_filter(df, blob)
    #         if schema_error_records:
    #             self.logger.warning(f"Found {len(schema_error_records)} schema mismatch errors")
    #             self.load_error_records(schema_error_records, blob)
            
    #         # Continue processing with valid rows
    #         if valid_df.empty:
    #             self.logger.warning("No valid rows to process after schema validation")
    #             return None
            
    #         self.logger.info(f"Processing {len(valid_df)} valid rows")

    #         # Get phone and email column configurations
    #         phone_column = self.dag_config.get('phone_column')
    #         email_column = self.dag_config.get('email_column')
            
    #         # Check if we have either phone or email columns to process
    #         if (phone_column and phone_column in valid_df.columns) or (email_column and email_column in valid_df.columns):
    #             enriched_df, id_service_errors = self.enrich_with_mdlz_ids(valid_df, phone_column, email_column, blob.name)
                
    #             # Log the state before error processing
    #             successful_before_removal = len(enriched_df[enriched_df['mdlzID'].notna()])
    #             hashed_before_removal = len(enriched_df[(enriched_df['mdlzID'].isna()) & (enriched_df.apply(self.has_hashed_identifiers, axis=1))])
    #             self.logger.info(f"Before error removal: {successful_before_removal} records with mdlzID, {hashed_before_removal} records with hashed identifiers")
                
    #             if id_service_errors:
    #                 self.logger.warning(f"Failed to fetch mdlzIDs for {len(id_service_errors)} records")
    #                 self.load_error_records(id_service_errors, blob)
                    
    #                 # CORRECTED LOGIC: Don't remove rows with hashed identifiers
    #                 # The enrich_with_mdlz_ids method already handles the logic correctly
    #                 # Records with hashed identifiers are kept in the dataframe with mdlzID=None
    #                 # Records that should be errors are already removed and added to error_records
                    
    #                 # No need to remove any additional rows here!
    #                 self.logger.info("Error records have been processed and sent to error table")
    #                 self.logger.info("Records with hashed identifiers remain in main dataframe with mdlzID=None")
                    
    #         else:
    #             enriched_df = valid_df.copy()
    #             enriched_df['normalized_phone'] = None
    #             enriched_df['normalized_email'] = None
    #             enriched_df['mdlzID'] = None
            
    #         if enriched_df.empty:
    #             self.logger.warning("No valid rows remaining after ID service processing")
    #             return None
            
    #         # Final composition logging
    #         final_successful_count = len(enriched_df)
    #         records_with_mdlzid = len(enriched_df[enriched_df['mdlzID'].notna()])
    #         records_with_hashes_only = len(enriched_df[enriched_df['mdlzID'].isna()])
            
    #         self.logger.info(f"Final main table composition: {final_successful_count} total records")
    #         self.logger.info(f"  - {records_with_mdlzid} records with mdlzID")
    #         self.logger.info(f"  - {records_with_hashes_only} records with hashed identifiers only (mdlzID=None)")
            
    #         final_columns = self.dag_config.get('final_columns', [])
    #         for col in final_columns:
    #             if col not in enriched_df.columns:
    #                 enriched_df[col] = None
    #         if final_columns:
    #             enriched_df = enriched_df[final_columns]
    #             self.logger.info(f"Reordered columns to match final schema: {final_columns}")
            
    #         transformed_df = self.transform_to_json_format(enriched_df, blob)
    #         tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    #         transformed_df.to_csv(tmp.name, index=False)
    #         self.logger.info(f"Processed CSV saved to temporary file: {tmp.name}")
    #         return tmp.name
            
    #     except Exception as e:
    #         traceback.print_exc()
    #         self.logger.error(f"Error in processing CSV file: {e}")
    #         raise e
    

    def process_csv_file(self, blob: storage.Blob, identifier: str) -> Optional[str]:
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            self.logger.info(f"Processing CSV file from SFTP: {blob.name} for {identifier} identifier @ {record_created_at}")

            content = blob.download_as_bytes()
            df = self.read_csv_with_proper_dtypes(content)

            # ENHANCED: Apply selective column mapping based on what's actually received
            column_mappings = self.dag_config.get('column_mappings', {})
            if column_mappings:
                # Identify which columns from mappings are actually present in the file
                received_mapped_columns, missing_mapped_columns, received_column_mapping = self.identify_received_columns(df)
                
                # Apply mapping only for received columns
                if received_column_mapping:
                    df.rename(columns=received_column_mapping, inplace=True)
                    self.logger.info(f"Applied selective column mappings for received columns: {received_column_mapping}")
                else:
                    self.logger.warning("No columns from column_mappings found in the file!")
                
                # Add missing columns as NULL to maintain schema consistency
                # for missing_col in missing_mapped_columns:
                #     df[missing_col] = None
                #     self.logger.debug(f"Added missing column '{missing_col}' as NULL")
                # Add missing columns as NULL to maintain schema consistency, but DO NOT overwrite existing columns
                for missing_col in missing_mapped_columns:
                    if missing_col not in df.columns:
                        df[missing_col] = None
                        self.logger.debug(f"Added missing column '{missing_col}' as NULL")
                    else:
                        self.logger.debug(f"Skipped adding missing column '{missing_col}' as it already exists after mapping")

            raw_file_df_columns = df.columns.to_list()
            self.logger.info(f"CSV file columns after selective mapping: {raw_file_df_columns}")
            
            # Check for schema mismatch and filter valid rows
            schema_error_records, valid_df = self.check_schema_mismatch_and_filter(df, blob)
            if schema_error_records:
                self.logger.warning(f"Found {len(schema_error_records)} schema mismatch errors")
                self.load_error_records(schema_error_records, blob)
            
            # Continue processing with valid rows
            if valid_df.empty:
                self.logger.warning("No valid rows to process after schema validation")
                return None
            
            self.logger.info(f"Processing {len(valid_df)} valid rows with selective column population")

            # Get phone and email column configurations
            phone_column = self.dag_config.get('phone_column')
            email_column = self.dag_config.get('email_column')
            
            # Check if we have either phone or email columns to process
            if (phone_column and phone_column in valid_df.columns) or (email_column and email_column in valid_df.columns):
                enriched_df, id_service_errors = self.enrich_with_mdlz_ids(valid_df, phone_column, email_column, blob.name)
                
                # Log the state before error processing
                successful_before_removal = len(enriched_df[enriched_df['mdlzID'].notna()])
                hashed_before_removal = len(enriched_df[(enriched_df['mdlzID'].isna()) & (enriched_df.apply(self.has_hashed_identifiers, axis=1))])
                self.logger.info(f"Before error removal: {successful_before_removal} records with mdlzID, {hashed_before_removal} records with hashed identifiers")
                
                if id_service_errors:
                    self.logger.warning(f"Failed to fetch mdlzIDs for {len(id_service_errors)} records")
                    self.load_error_records(id_service_errors, blob)
                    
                    # CORRECTED LOGIC: Don't remove rows with hashed identifiers
                    # The enrich_with_mdlz_ids method already handles the logic correctly
                    # Records with hashed identifiers are kept in the dataframe with mdlzID=None
                    # Records that should be errors are already removed and added to error_records
                    
                    # No need to remove any additional rows here!
                    self.logger.info("Error records have been processed and sent to error table")
                    self.logger.info("Records with hashed identifiers remain in main dataframe with mdlzID=None")
                    
            else:
                enriched_df = valid_df.copy()
                enriched_df['normalized_phone'] = None
                enriched_df['normalized_email'] = None
                enriched_df['mdlzID'] = None
            
            if enriched_df.empty:
                self.logger.warning("No valid rows remaining after ID service processing")
                return None
            
            # Final composition logging
            final_successful_count = len(enriched_df)
            records_with_mdlzid = len(enriched_df[enriched_df['mdlzID'].notna()])
            records_with_hashes_only = len(enriched_df[enriched_df['mdlzID'].isna()])
            
            self.logger.info(f"Final main table composition: {final_successful_count} total records")
            self.logger.info(f"  - {records_with_mdlzid} records with mdlzID")
            self.logger.info(f"  - {records_with_hashes_only} records with hashed identifiers only (mdlzID=None)")
            
            final_columns = self.dag_config.get('final_columns', [])
            for col in final_columns:
                if col not in enriched_df.columns:
                    enriched_df[col] = None
            if final_columns:
                enriched_df = enriched_df[final_columns]
                self.logger.info(f"Reordered columns to match final schema: {final_columns}")
            
            # ENHANCED: Transform with selective population
            transformed_df = self.transform_to_json_format(enriched_df, blob)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            transformed_df.to_csv(tmp.name, index=False)
            self.logger.info(f"Processed CSV with selective column population saved to temporary file: {tmp.name}")
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
            
            # Store original row data before any processing
            original_row_data = {}
            row_to_identifiers = {}  # Map row index to its identifiers
            phone_personas = []
            email_personas = []
            
            # CRITICAL: Track which rows should be removed as errors
            error_row_indices = set()
            
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
                        phone_value = phone_raw  # Don't pre-normalize phone
                        # Normalize phone for ID service
                        # if not phone_raw.startswith('+91'):
                        #     if phone_raw.startswith('0'):
                        #         phone_raw_without_zero = phone_raw[1:]
                        #         phone_value = f'+91{phone_raw_without_zero}'
                        #     else:
                        #         phone_value = f'+91{phone_raw}'
                        # else:
                        #     phone_value = phone_raw
                
                # Get and clean email value
                if email_column and email_column in df.columns:
                    email_raw = self.clean_email_address(row.get(email_column))
                    if email_raw:
                        original_email = email_raw
                        email_value = email_raw  # Don't pre-normalize email
                
                # Check if record has hashed identifiers (regardless of phone/email validity)
                has_hashed_identifiers = self.has_hashed_identifiers(row)
                
                # Store identifier mapping for this row with original data
                row_to_identifiers[index] = {
                    'phone': phone_value,
                    'email': email_value,
                    'original_phone': original_phone,
                    'original_email': original_email,
                    'has_phone': phone_value is not None,
                    'has_email': email_value is not None,
                    'has_hashed_identifiers': has_hashed_identifiers,
                    'original_data': original_row_data[index]
                }
                
                # Collect personas for batch processing ONLY if we have phone/email
                if phone_value:
                    phone_persona = PersonaFields(phone=phone_value, email=None)
                    phone_personas.append((phone_persona, index))
                
                if email_value:
                    email_persona = PersonaFields(phone=None, email=email_value)
                    email_personas.append((email_persona, index))
            
            # Step 1: Process all phone personas first - FIXED to handle 3 return values
            phone_success = {}
            phone_errors = {}
            phone_normalized_lookup = {}
            
            if phone_personas:
                self.logger.info(f"Step 1: Processing {len(phone_personas)} phone personas")
                phone_persona_list = [persona for persona, _ in phone_personas]
                phone_success, phone_errors, phone_normalized_lookup = asyncio.run(fetch_mdlz_ids_parallel_personas(phone_persona_list))
                self.logger.info(f"Phone results: {len(phone_success)} successful, {len(phone_errors)} errors")
            
            # Step 2: Process all email personas - FIXED to handle 3 return values
            email_success = {}
            email_errors = {}
            email_normalized_lookup = {}
            
            if email_personas:
                self.logger.info(f"Step 2: Processing {len(email_personas)} email personas")
                email_persona_list = [persona for persona, _ in email_personas]
                email_success, email_errors, email_normalized_lookup = asyncio.run(fetch_mdlz_ids_parallel_personas(email_persona_list))
                self.logger.info(f"Email results: {len(email_success)} successful, {len(email_errors)} errors")
            
            # Step 3: Apply CORRECTED phone-first priority logic for each row
            for index, row in df.iterrows():
                identifiers = row_to_identifiers[index]
                phone_value = identifiers['phone']
                email_value = identifiers['email']
                has_phone = identifiers['has_phone']
                has_email = identifiers['has_email']
                has_hashed_identifiers = identifiers['has_hashed_identifiers']
                original_phone = identifiers['original_phone']
                original_email = identifiers['original_email']
                
                mdlz_id = None
                should_create_error = False
                
                # FIXED: Set normalized values using the lookup dictionaries from ID service
                if original_phone:
                    df.at[index, 'normalized_phone'] = phone_normalized_lookup.get(original_phone, phone_value)
                if original_email:
                    df.at[index, 'normalized_email'] = email_normalized_lookup.get(original_email, email_value)
                
                # CORRECTED LOGIC: Check ID service results first
                if has_phone and phone_value in phone_success:
                    # Phone succeeded
                    mdlz_id = phone_success[phone_value]
                    self.logger.debug(f"Row {index}: Phone {phone_value} succeeded -> {mdlz_id}")
                    
                elif has_email and email_value in email_success:
                    # Email succeeded (either phone failed or no phone)
                    mdlz_id = email_success[email_value]
                    if has_phone:
                        self.logger.debug(f"Row {index}: Phone failed, email {email_value} succeeded -> {mdlz_id}")
                    else:
                        self.logger.debug(f"Row {index}: Email-only {email_value} succeeded -> {mdlz_id}")
                
                elif has_phone or has_email:
                    # We have phone/email but ID service failed for both
                    should_create_error = True
                    error_row_indices.add(index)  # CRITICAL: Mark for removal
                    self.logger.debug(f"Row {index}: ID service failed for phone={phone_value}, email={email_value}")
                
                elif has_hashed_identifiers:
                    # CORRECTED: Only keep in main table if NO phone/email but has hashed identifiers
                    mdlz_id = None  # Keep in main table with null mdlzID
                    self.logger.debug(f"Row {index}: No phone/email but has hashed identifiers - keeping in main table")
                
                else:
                    # No identifiers at all
                    should_create_error = True
                    error_row_indices.add(index)  # CRITICAL: Mark for removal
                    self.logger.debug(f"Row {index}: No identifiers at all")
                
                # Set mdlzID or create error record
                if should_create_error:
                    self.create_detailed_error_record_with_original_data(
                        index, identifiers, phone_success, phone_errors, 
                        email_success, email_errors, error_records
                    )
                else:
                    df.at[index, 'mdlzID'] = mdlz_id
            
            # CRITICAL FIX: Remove error rows from the main dataframe
            if error_row_indices:
                self.logger.info(f"Removing {len(error_row_indices)} error rows from main dataframe")
                df = df.drop(error_row_indices)
                self.logger.info(f"Main dataframe now has {len(df)} rows after error removal")
            
            # Count results
            successful_count = len(df[df['mdlzID'].notna()])
            hashed_only_count = len(df[(df['mdlzID'].isna()) & (df.apply(lambda row: row_to_identifiers[row.name]['has_hashed_identifiers'] and not row_to_identifiers[row.name]['has_phone'] and not row_to_identifiers[row.name]['has_email'], axis=1))])
            error_count = len(error_records)
            original_total_count = len(original_row_data)  # Use original count before removal
            
            self.logger.info(f"ID enrichment summary:")
            self.logger.info(f"  - {successful_count} records with mdlzID (ID service succeeded)")
            self.logger.info(f"  - {hashed_only_count} records with hashed identifiers only (no phone/email)")
            self.logger.info(f"  - {error_count} error records (ID service failed or no identifiers)")
            self.logger.info(f"  - {original_total_count} total input records")
            self.logger.info(f"  - {len(df)} records remaining in main dataframe")
            
            # Validation check
            if successful_count + hashed_only_count + error_count != original_total_count:
                self.logger.error(f"DATA LOSS DETECTED! Input: {original_total_count}, Output: {successful_count + hashed_only_count + error_count}")
                raise Exception(f"Data loss detected: {original_total_count - (successful_count + hashed_only_count + error_count)} records missing")
            
            # Additional validation: Main dataframe should only contain successful + hashed_only records
            expected_main_count = successful_count + hashed_only_count
            if len(df) != expected_main_count:
                self.logger.error(f"MAIN DATAFRAME SIZE MISMATCH! Expected: {expected_main_count}, Actual: {len(df)}")
                raise Exception(f"Main dataframe size mismatch: expected {expected_main_count}, got {len(df)}")
            
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


    def create_detailed_error_record_with_original_data(self, row_index: int, identifiers: Dict, 
                                               phone_success: Dict, phone_errors: Dict, 
                                               email_success: Dict, email_errors: Dict, 
                                               error_records: List[Dict]) -> None:
        """Create detailed error record using original stored data to prevent data mixing."""
        
        phone_value = identifiers['phone']
        email_value = identifiers['email']
        has_phone = identifiers['has_phone']
        has_email = identifiers['has_email']
        has_hashed_identifiers = identifiers['has_hashed_identifiers']
        original_phone = identifiers['original_phone']
        original_email = identifiers['original_email']
        
        # CORRECTED: Don't skip error record creation for hashed identifiers
        # Only skip if record has hashed identifiers AND no phone/email
        if has_hashed_identifiers and not has_phone and not has_email:
            self.logger.debug(f"Skipping error record creation for row {row_index} - has hashed identifiers but no phone/email")
            return
        
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
        
        # Add note about hashed identifiers if present
        if has_hashed_identifiers:
            error_detail += "Record has hashed identifiers but phone/email validation failed. "
        
        error_record = {
            'phone': original_phone,
            'email': original_email,
            'error_detail': error_detail.strip(),
            'data': sftp_data_with_consent
        }
        error_records.append(error_record)
        
        # Enhanced logging for debugging
        self.logger.debug(f"Created error record for row {row_index}: phone={original_phone}, email={original_email}, has_hashed={has_hashed_identifiers}")


    def create_service_failure_error_record(self, df: pd.DataFrame, row_index: int, 
                                    row_to_identifiers: Dict, error_msg: str, 
                                    error_records: List[Dict]) -> None:
        """Create error record for service failures."""
        row = df.iloc[row_index]
        
        # Get identifiers if available
        identifiers = row_to_identifiers.get(row_index, {})
        has_phone = identifiers.get('has_phone', False)
        has_email = identifiers.get('has_email', False)
        has_hashed_identifiers = identifiers.get('has_hashed_identifiers', False)
        
        # CORRECTED: Only skip if record has hashed identifiers AND no phone/email
        if has_hashed_identifiers and not has_phone and not has_email:
            self.logger.debug(f"Skipping service failure error record for row {row_index} - has hashed identifiers but no phone/email")
            return
        
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
        original_phone = identifiers.get('original_phone')
        original_email = identifiers.get('original_email')
        
        # Add channel level consent to error record data
        sftp_data_with_consent = self.add_channel_level_consent(
            sftp_data, original_phone, original_email
        )
        
        error_detail = f"ID service error: {error_msg}"
        if has_hashed_identifiers:
            error_detail += " Record has hashed identifiers but service failed."
        
        error_record = {
            'phone': original_phone,
            'email': original_email,
            'error_detail': error_detail,
            'data': sftp_data_with_consent
        }
        error_records.append(error_record)