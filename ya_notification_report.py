import asyncio
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


class YaNotificationReport:
    def __init__(self, dag_name: str):
        self.config = Config()
        self.logger = get_logger()
        self.dag_name = dag_name
        self.dag_config = self.config.get_dag_config(dag_name)
        self.bq_client_data = bigquery.Client(project=self.dag_config['target_project'])
        if not self.dag_config:
            available_dags = self.config.list_available_dags()
            raise ValueError(f"DAG configuration not found for '{dag_name}'. Available DAGs: {available_dags}")
        self.logger.info(f'Initialized for {self.dag_name}')


    @log_execution_time(threshold=0.1)
    def get_bot_id_list(self):
        try:
            lookup_table = f'{self.dag_config['target_project']}.{self.dag_config['lookup_dataset']}.{self.dag_config['lookup_table']}'          
            self.logger.info(f"Fetching bot_id_list from lookup table {lookup_table}")
            fetch_botid_query = f"""SELECT bot_id FROM `{lookup_table}` WHERE env_name = 'production'"""
            query_job = self.bq_client_data.query(fetch_botid_query)
            query_result = query_job.result()        
            bot_id_list = [row.bot_id for row in query_result]
            return bot_id_list
        except Exception as e:
            traceback.print_exc()
            self.error(f"Error in fetching bot_id_list: {e}")
            raise e
    
    
    # def list_prefixes(self) -> List[Tuple[Optional[str], str]]:
    #     """
    #     Returns a list of (unit, prefix) pairs.
    #     All prefix patterns are driven by config['prefix_patterns'], substituting:
    #      - 'unit': if use_units=True, each element from get_bot_id_list()
    #      - any other keys from config['pattern_params']
    #     """
    #     config = self.dag_config
    #     patterns = config.get('prefix_patterns', [])
    #     params = config.get('pattern_params', {})
    #     use_units = config.get('use_units', False)

    #     self.logger.info(f"prefix_patterns: {patterns}")
    #     self.logger.info(f"pattern_params: {params}")
    #     self.logger.info(f"use_units flag: {use_units}")

    #     prefixes: List[Tuple[Optional[str], str]] = []

    #     # 1) Determine units (or a single None)
    #     if use_units:
    #         units = self.get_bot_id_list()
    #     else:
    #         units = [None]

    #     # 2) For each pattern, for each unit, do a .format(**params, unit=unit)
    #     for patt in patterns:
    #         for unit in units:
    #             fmt_params = dict(params)  # copy static params
    #             fmt_params['unit'] = unit or ''
    #             try:
    #                 prefix = patt.format(**fmt_params)
    #             except KeyError as ke:
    #                 raise ValueError(f"Pattern '{patt}' failed; missing param {ke}")
    #             prefixes.append((unit, prefix))

    #     # 3) Deduplicate and return
    #     #    (in case patterns overlap)
    #     seen = set()
    #     unique: List[Tuple[Optional[str], str]] = []
    #     for u, p in prefixes:
    #         if (u, p) not in seen:
    #             unique.append((u, p))
    #             seen.add((u, p))

    #     return unique

    def generate_unit_prefix_pairs(self) -> List[Tuple[Optional[str], str]]:
        """
        Generate a list of (unit, prefix) pairs based on configuration patterns.
        
        This method uses the configuration to create prefix patterns by substituting:
        - 'unit': if use_units=True, each element from get_bot_id_list()
        - any other keys from config['pattern_params']
        """
        config = self.dag_config
        prefix_patterns = config.get('prefix_patterns', [])
        pattern_parameters = config.get('pattern_params', {})
        include_units = config.get('use_units', False)

        self.logger.info(f"Prefix patterns: {prefix_patterns}")
        self.logger.info(f"Pattern parameters: {pattern_parameters}")
        self.logger.info(f"Include units flag: {include_units}")

        unit_prefix_pairs: List[Tuple[Optional[str], str]] = []

        # Determine units (or a single None)
        units = self.get_bot_id_list() if include_units else [None]

        # Generate prefix for each pattern and unit
        for pattern in prefix_patterns:
            for unit in units:
                formatted_params = dict(pattern_parameters)  # copy static params
                formatted_params['unit'] = unit or ''
                try:
                    prefix = pattern.format(**formatted_params)
                except KeyError as ke:
                    raise ValueError(f"Pattern '{pattern}' failed; missing parameter {ke}")
                unit_prefix_pairs.append((unit, prefix))

        # Deduplicate and return unique pairs
        seen_pairs = set()
        unique_pairs: List[Tuple[Optional[str], str]] = []
        for unit, prefix in unit_prefix_pairs:
            if (unit, prefix) not in seen_pairs:
                unique_pairs.append((unit, prefix))
                seen_pairs.add((unit, prefix))

        return unique_pairs
        
    @log_execution_time(threshold=5.0)
    def process_csv_file(self, blob: storage.Blob, bot_prefix: str) -> str:
        """
        Generic CSV processing method that handles different schemas based on configuration.
        
        Args:
            blob: GCS blob object
            bot_prefix: Bot identifier for processing
            
        Returns:
            Path to processed temporary CSV file
        """
        try:
            self.logger.info(f"Processing CSV file {blob.name} for bot {bot_prefix}")
            content = blob.download_as_bytes()
            df = pd.read_csv(pd.io.common.BytesIO(content), low_memory=False)
            
            # Apply column mappings if specified
            column_mappings = self.dag_config.get('column_mappings', {})
            if column_mappings:
                df.rename(columns=column_mappings, inplace=True)
                self.logger.info(f"Applied column mappings: {column_mappings}")
            
            # Filter to expected schema columns
            expected_schema = self.dag_config.get('expected_columns', [])
            if expected_schema:
                available_columns = [col for col in expected_schema if col in df.columns]
                df = df[available_columns]
                self.logger.info(f"Filtered to expected columns: {available_columns}")
            
            # Add bot_id if required
            if self.dag_config.get('add_bot_id', False):
                df['bot_id'] = bot_prefix
                self.logger.info(f"Added bot_id column with value: {bot_prefix}")
            
            # Handle phone number enrichment
            phone_column = self.dag_config.get('phone_column')
            if phone_column and phone_column in df.columns:
                df = self.enrich_with_mdlz_ids(df, phone_column, blob.name)
            
            # Add any additional static columns
            additional_columns = self.dag_config.get('additional_columns', {})
            for col_name, col_value in additional_columns.items():
                df[col_name] = col_value
                self.logger.info(f"Added additional column {col_name} with value: {col_value}")
            
            # Ensure all final columns exist
            final_columns = self.dag_config.get('final_columns', [])
            for col in final_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder columns to match schema
            if final_columns:
                df = df[final_columns]
                self.logger.info(f"Reordered columns to match final schema: {final_columns}")
            
            # Save to temporary file
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            df.to_csv(tmp.name, index=False)
            self.logger.info(f"Processed CSV saved to temporary file: {tmp.name}")
            return tmp.name
            
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error processing blob {blob.name} for bot {bot_prefix}: {e}")
            raise e
    

    def enrich_with_mdlz_ids(self, df: pd.DataFrame, phone_column: str, blob_name: str) -> pd.DataFrame:
        """Enrich dataframe with Mondelez IDs based on phone numbers."""
        try:
            self.logger.info(f"Fetching mdlzIds for {phone_column} in {blob_name}")
            df['normalized_phone'] = '+' + df[phone_column].astype(str).str.lstrip('+')
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