from helpers.log import get_logger
from bq.bq_logic import BQLogic
from api.neverbounce_api_connector import NeverbounceApiConnector
import os
from datetime import datetime
from bq.models.audit_table_schema import audit_table_schema
import math

class ExecuteValidation:
    def __init__(self,execution_id=None):
        self.logger = get_logger()
        self.bq_logic = BQLogic()
        self.execution_id = execution_id
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        self.lookup_dataset = os.environ.get("LOOKUP_DATASET")
        self.source_lookup_table = os.environ.get("SOURCE_LOOKUP_TABLE")
        self.lookup_table = os.environ.get("LOOKUP_TABLE")
        self.udf_path = f"{self.project_id}.{os.environ.get('UDF_PATH')}"
        self.structure_merge_udf_path = os.environ.get('STRUCTURE_MERGE_UDF_PATH')
        self.logging_dataset = os.environ.get("LOGGING_DATASET")
        self.audit_table = os.environ.get("AUDIT_TABLE")
        self.max_batch_size = os.environ.get("MAX_BATCH_SIZE")

    async def process_emails(self):
        validated_email_count = 0
        valid_count = 0
        invalid_count = 0
        catchall_count = 0
        unknown_count = 0
        disposable_count = 0
        start_time = datetime.utcnow().isoformat()
        try:
            neverbounce_api_key = self.bq_logic.initialize_config()
            neverbounce_connector = NeverbounceApiConnector(neverbounce_api_key)
            list_of_tables = self.bq_logic.get_table_list(self.project_id, self.lookup_dataset, self.source_lookup_table)
            sql = self.bq_logic.build_unvalidated_query(list_of_tables, self.project_id, self.udf_path)
            for chunk_id, chunk in self.bq_logic.fetch_unvalidated_emails(sql, self.max_batch_size):
                self.logger.info("Processing chunk_id=%s of size=%d", chunk_id,
                            len(chunk))
                fetch_lookup_emails_iterator = self.bq_logic.fetch_validation_from_lookup(self.project_id,
                                                                            self.lookup_dataset, self.lookup_table,
                                                                            chunk)
                fetch_lookup_emails_rows = list(fetch_lookup_emails_iterator)
                neverbounce_emails = self.bq_logic.get_neverbounce_emails(fetch_lookup_emails_rows)
                neverbounce_results = []
                if "emails" in neverbounce_emails:
                    emails = neverbounce_emails.get("emails")
                    if not isinstance(emails, list):
                        raise
                    neverbounce_results = await neverbounce_connector.verify_emails_bulk(emails, chunk_id=chunk_id)

                validated_email_count += len(neverbounce_results)

                for result in neverbounce_results:
                    if result["result"] == 'valid':
                        valid_count += 1
                    if result["result"] == 'invalid':
                        invalid_count += 1
                    if result["result"] == 'catchall':
                        catchall_count += 1
                    if result["result"] == 'unknown':
                        unknown_count += 1
                    if result["result"] == 'disposable':
                        disposable_count += 1
                merge_source_iterator = self.bq_logic.prepare_merge_source(neverbounce_results, fetch_lookup_emails_rows)
                merge_source_rows = list(merge_source_iterator)
                self.bq_logic.insert_in_lookup(self.project_id, self.lookup_dataset, self.lookup_table, neverbounce_results)
                merge_json_structure_iterator = self.bq_logic.structure_merge_source(self.project_id,
                                                                                     self.structure_merge_udf_path,
                                                                                     merge_source_rows)
                merge_json_structure_rows = list(merge_json_structure_iterator)
                self.bq_logic.merge_into_hmz_tables(self.project_id, list_of_tables, merge_json_structure_rows)

                self.logger.info("Neverbounce process completed for chunk_id=%s of size=%d", chunk_id,
                                 len(chunk))

            end_time = datetime.utcnow().isoformat()
            audit_json_list = self.bq_logic.build_audit_json_list(start_time,end_time,validated_email_count,valid_count,
                                                                  invalid_count,catchall_count,unknown_count,
                                                                  disposable_count,self.execution_id)
            self.logger.info(f"[DEBUG] AUDIT JSON BEFORE SUCCESS INSERT: {audit_json_list}")
            self.logger.info(f"[DEBUG] DATASET={self.logging_dataset}, TABLE={self.audit_table}")
            self.bq_logic.batch_insert_json_to_audit_table(audit_json_list,self.logging_dataset,
                                                           self.audit_table,audit_table_schema)

        except Exception as e:
            self.logger.error(f"Neverbounce Validation failed withe error - {e}")
            end_time = datetime.utcnow().isoformat()
            audit_json_list = self.bq_logic.build_audit_json_list(start_time,end_time,validated_email_count,valid_count,
                                                                  invalid_count,catchall_count,unknown_count,
                                                                  disposable_count,self.execution_id)
            self.logger.error(f"[DEBUG] AUDIT JSON BEFORE FAILURE INSERT: {audit_json_list}")
            self.logger.error(f"[DEBUG] DATASET={self.logging_dataset}, TABLE={self.audit_table}")
            self.bq_logic.batch_insert_json_to_audit_table(audit_json_list,self.logging_dataset,
                                                           self.audit_table,audit_table_schema)
            raise