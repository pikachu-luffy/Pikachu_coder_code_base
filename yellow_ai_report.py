import tempfile
import asyncio
import traceback
import pendulum
import pandas as pd
import uuid
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from api.id_service_connector import IdServiceConnector
from api.id_service_helper import fetch_mdlz_ids_parallel
from helpers.config import Config
from helpers.log import get_logger, log_execution_time
from bq.bq_connector import BQConnector



class YellowAIDataLoadJob:
    def __init__(self, **kwargs) -> None:
        self.config = Config()
        self.logger = get_logger()
        self.ya_bq_repository = BQConnector()
        self.gcs_storage_client = storage.Client()
        self.id_service_connector = IdServiceConnector()
        self.notification_gcs_prefix = self.config.app.notification_gcs_prefix
        self.notification_report_prefix = self.config.app.notification_report_prefix
        self.unsubscribe_gcs_prefix = self.config.app.unsubscribe_gcs_prefix
        self.unsubscribe_report_prefix = self.config.app.unsubscribe_report_prefix


    def get_blobs(self, landing_time: int, prefix) -> list[dict]:
        """Return list of GCS blobs with generation > since_generation."""
        try:
            self.logger.info("Fetching blobs which are to be processed")
            blob_list = self.gcs_storage_client.list_blobs(self.config.gcp.gcp_gcs_data_bucket, prefix=prefix)
            new_blob_list = []
            for blob in blob_list:
                if not blob.name.lower().endswith(".csv"):
                    continue
                blob_generation = int(blob.generation)
                if blob_generation > landing_time:
                    new_blob_list.append({
                        'name': blob.name,
                        'generation': blob_generation,
                        'updated': blob.updated,
                    })
                    self.logger.info(f"New blob: {blob.name} gen={blob_generation} updated={blob.updated}")
            self.logger.info(f"Total new blobs: {len(new_blob_list)}")
            return new_blob_list
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error listing new blobs: {e}")  
            raise e 
    
    @log_execution_time(threshold=5.0)
    def enrich_csv_mdlz_id(self, blob: str, bot_prefix: str) -> str:
        try:
            self.logger.info(f"Handling CSV file {blob} for bot {bot_prefix}")
            content = blob.download_as_bytes()
            df = pd.read_csv(pd.io.common.BytesIO(content), low_memory=False)
            if '_id' in df.columns:
                df.rename(columns={'_id': 'Id'}, inplace=True)
            expected_schema = ["Id","campaignId","botId","runId","userId","audienceId","status","statusDetail",
                                "internalStatus","comments","messageId","channelMsgId","templateId","customPayload",
                                "userDetails","currentCronTime","source","senderId","workflowId","updated",
                                "created","sentAt","deliveredAt","readAt","repliedAt","engagedAt","usedPlainText",
                                "errorResolution","errorMessage","rawError","ipAddress","sentBy","name",
                                "workflowStatus","workflowNodeId","workflowUserId","quickReplyResponse","workflowDetails",
                                "notificationPostbackUrl","subSource","postbackAuthHeader","cdpUserId","smsUnits","sessionStart",
                                "sessionType","sessionConversationId"]
            df = df[[col for col in expected_schema if col in df.columns]]
            if 'userId' in df.columns:
                self.logger.info(f"Fetching mdlzIds for userId in {blob.name}")
                df['normalized_phone'] = '+' + df['userId'].astype(str).str.lstrip('+')
                phones = df['normalized_phone'].dropna().astype(str).unique().tolist()
                if phones:
                    phone_to_mdlzid = asyncio.run(fetch_mdlz_ids_parallel(phones))
                    df['mdlzID'] = df['normalized_phone'].map(phone_to_mdlzid)
                else:
                    df['mdlzID'] = None
            else:
                df['normalized_phone'] = None
                df['mdlzID'] = None
                    # df.rename(columns={"normalized_phone": "normalized_phone"}, inplace=True)
                # df.drop(columns=['normalized_phone'], inplace=True, errors='ignore')
            final_expected_columns = [
            "Id", "campaignId", "botId", "runId", "userId", "audienceId", "status", "statusDetail",
            "internalStatus", "comments", "messageId", "channelMsgId", "templateId", "customPayload",
            "userDetails", "currentCronTime", "source", "senderId", "workflowId", "updated",
            "created", "sentAt", "deliveredAt", "readAt", "repliedAt", "engagedAt", "usedPlainText",
            "errorResolution", "errorMessage", "rawError", "ipAddress", "sentBy", "name",
            "workflowStatus", "workflowNodeId", "workflowUserId", "quickReplyResponse", "workflowDetails",
            "notificationPostbackUrl", "subSource", "postbackAuthHeader", "cdpUserId", "smsUnits", 
            "sessionStart", "sessionType", "sessionConversationId", "mdlzID", "normalized_phone"
        ]
        
        # Add missing columns with None values
            for col in final_expected_columns:
                if col not in df.columns:
                    df[col] = None
        
        # Reorder columns to match BigQuery schema exactly
            df = df[final_expected_columns]
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            df.to_csv(tmp.name, index=False)
            return tmp.name
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error processing blob {blob} for bot {bot_prefix}: {e}")
            raise e


    def enrich_csv_with_bot_id_and_mdlz_id(self, blob: str, bot_prefix: str) -> str:
        try:
            self.logger.info(f"Handling CSV file {blob} for bot {bot_prefix}")
            content = blob.download_as_bytes()
            df = pd.read_csv(pd.io.common.BytesIO(content))
            df['bot_id'] = bot_prefix
            if 'phone' in df.columns:
                self.logger.info(f"Fetching mdlzIds for phones in {blob.name}")
                df['normalized_phone'] = '+' + df['phone'].astype(str).str.lstrip('+')
                phones = df['normalized_phone'].dropna().astype(str).unique().tolist()
                if phones:
                    phone_to_mdlzid = asyncio.run(fetch_mdlz_ids_parallel(phones))
                    df['mdlzID'] = df['normalized_phone'].map(phone_to_mdlzid)
                else:
                    df['mdlzID'] = None
            else:
                df['normalized_phone'] = None
                df['mdlzID'] = None
            
            expected_columns = ["phone", "user_request", "created_at", "updated_at", "bot_id", "mdlzID", "normalized_phone"]

            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
        
        # Reorder columns to match schema
            df = df[expected_columns]
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            df.to_csv(tmp.name, index=False)
            return tmp.name
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error processing blob {blob} for bot {bot_prefix}: {e}")
            raise e

    def load_unsubscribe_report_data_to_bigquery(self):
        """
        Main function to load data from GCS to BigQuery.
        This function checks for new files in GCS, processes them, and loads the data into BigQuery.
        It also updates the audit table with the latest processed generation and timestamps.
        """
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            audit_rows = []
            bot_id_list = self.ya_bq_repository.get_bot_id_list()
            for bot_id in bot_id_list:
                self.logger.info("Processing bot prefix: %s", bot_id)
                last_processed_epoch_ts = self.ya_bq_repository.fetch_last_processed_epoch_ts(bot_id, self.config.app.unsubscribe_report_prefix)
                self.logger.info("Last processed epoch timestamp for bot %s: %s", bot_id, last_processed_epoch_ts)
                prefix = f"{bot_id}/{self.unsubscribe_gcs_prefix}"
                blob_list = self.get_blobs(last_processed_epoch_ts, prefix)
                if not blob_list:
                    self.logger.info("No new files to process for bot %s. Exiting.", bot_id)
                    continue
                for blob_info in blob_list:
                    file_processing_start_time = datetime.now(pendulum.UTC)
                    blob = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).get_blob(blob_info['name'])
                    total_rows_loaded = 0
                    file_load_status = 'FAILURE'
                    load_error_message = None
                    try:
                        local_path = self.enrich_csv_with_bot_id_and_mdlz_id(blob, bot_id)
                        processed_blob_name = f'processed_files/{blob_info["name"]}'
                        processed_blob = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).blob(processed_blob_name)
                        processed_blob.upload_from_filename(local_path)
                        self.logger.info("Uploading processed file to GCS: %s", processed_blob_name)
                        gcs_uri = f"gs://{self.config.gcp.gcp_gcs_data_bucket}/{processed_blob_name}"
                        self.logger.info("Loading processed files from GCS: %s", gcs_uri)
                        schema = [
                                            bigquery.SchemaField("phone", "STRING"),	
                                            bigquery.SchemaField("user_request", "STRING"),	
                                            bigquery.SchemaField("created_at", "TIMESTAMP"),	
                                            bigquery.SchemaField("updated_at","TIMESTAMP"),
                                            bigquery.SchemaField("bot_id", "STRING"),
                                            bigquery.SchemaField("mdlzID", "STRING"),
                                            bigquery.SchemaField("normalized_phone", "STRING")
                                        ]
                        total_rows_loaded = self.ya_bq_repository.load_table_from_gcs_to_bq(gcs_uri=gcs_uri, table_name=f"{self.config.gcp.gcp_bq_data_project_id}.{self.config.gcp.gcp_bq_dataset_id}.{self.config.gcp.gcp_bq_unsubscribe_table}", schema=schema)
                        if total_rows_loaded:
                            self.logger.info("Loaded %s rows for bot %s.", total_rows_loaded, bot_id)
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
                        'execution_id': str(uuid.uuid4()),
                        'process_name': f'in_ya_{self.config.app.unsubscribe_report_prefix}',
                        'source_name': str(self.config.gcp.gcp_gcs_data_bucket),
                        'load_type': 'File',
                        'load_source_type': 'GCS',
                        'source_detail': blob_info['name'],
                        'target_table': self.config.gcp.gcp_bq_unsubscribe_table,
                        'landing_time': blob_info['generation'],
                        'process_start_time': file_processing_start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_end_time': file_processing_end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_status': file_load_status,
                        'rows_processed': total_rows_loaded,
                        'error_message': load_error_message,
                        'created_at': record_created_at,
                        'updated_at': datetime.now(pendulum.UTC).strftime('%Y-%m-%dT%H:%M:%S')
                    })

            if audit_rows:
                self.ya_bq_repository.insert_audit_records(audit_rows)
                processed_folder = 'processed_files/'
                processed_blobs = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).list_blobs(prefix=processed_folder)
                for processed_blob in processed_blobs:
                    self.logger.info(f"Deleting processed blob: {processed_blob.name}")
                    processed_blob.delete()
                self.logger.info(f'Successfully deleted all the  processed blobs for the bucket')
            else:
                self.logger.info(f"No audit records to insert, skipping insert_audit_records.")
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in load_data_to_bigquery: {e}")
            raise e

    def load_notification_report_data_to_bigquery(self):
        """
        Main function to load data from GCS to BigQuery.
        This function checks for new files in GCS, processes them, and loads the data into BigQuery.
        It also updates the audit table with the latest processed generation and timestamps.
        """
        try:
            pipeline_load_start = datetime.now(pendulum.UTC)
            record_created_at = pipeline_load_start.strftime('%Y-%m-%dT%H:%M:%S')
            audit_rows = []
            bot_id_list = self.ya_bq_repository.get_bot_id_list()
            for bot_id in bot_id_list:
                self.logger.info("Processing bot prefix: %s", bot_id)
                last_processed_epoch_ts = self.ya_bq_repository.fetch_last_processed_epoch_ts(bot_id, self.config.app.notification_report_prefix)
                self.logger.info("Last processed epoch timestamp for bot %s: %s", bot_id, last_processed_epoch_ts)
                prefix = f"{bot_id}/{self.notification_gcs_prefix}"
                blob_list = self.get_blobs(last_processed_epoch_ts, prefix)
                if not blob_list:
                    self.logger.info("No new files to process for bot %s. Exiting.", bot_id)
                    continue
                for blob_info in blob_list:
                    file_processing_start_time = datetime.now(pendulum.UTC)
                    blob = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).get_blob(blob_info['name'])
                    total_rows_loaded = 0
                    file_load_status = 'FAILURE'
                    load_error_message = None
                    try:
                        local_path = self.enrich_csv_mdlz_id(blob, bot_id)
                        processed_blob_name = f'processed_files/{blob_info["name"]}'
                        processed_blob = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).blob(processed_blob_name)
                        processed_blob.upload_from_filename(local_path)
                        self.logger.info("Uploading processed file to GCS: %s", processed_blob_name)
                        gcs_uri = f"gs://{self.config.gcp.gcp_gcs_data_bucket}/{processed_blob_name}"
                        self.logger.info("Loading processed files from GCS: %s", gcs_uri)
                        schema = [
                                    bigquery.SchemaField("Id",		"STRING"),
                                    bigquery.SchemaField("campaignId",		"STRING"),
                                    bigquery.SchemaField("botId",		"STRING"),
                                    bigquery.SchemaField("runId",		"STRING"),
                                    bigquery.SchemaField("userId",		"STRING"),
                                    bigquery.SchemaField("audienceId",		"STRING"),
                                    bigquery.SchemaField("status",		"STRING"),
                                    bigquery.SchemaField("statusDetail",		"STRING"),
                                    bigquery.SchemaField("internalStatus",		"STRING"),
                                    bigquery.SchemaField("comments",		"STRING"),
                                    bigquery.SchemaField("messageId",		"STRING"),
                                    bigquery.SchemaField("channelMsgId",		"STRING"),
                                    bigquery.SchemaField("templateId",		"STRING"),
                                    bigquery.SchemaField("customPayload",		"STRING"),
                                    bigquery.SchemaField("userDetails",		"STRING"),
                                    bigquery.SchemaField("currentCronTime",		"TIMESTAMP"),
                                    bigquery.SchemaField("source",		"STRING"),
                                    bigquery.SchemaField("senderId",		"STRING"),
                                    bigquery.SchemaField("workflowId",		"STRING"),
                                    bigquery.SchemaField("updated",		"TIMESTAMP"),
                                    bigquery.SchemaField("created",		"TIMESTAMP"),
                                    bigquery.SchemaField("sentAt",		"TIMESTAMP"),
                                    bigquery.SchemaField("deliveredAt",		"TIMESTAMP"),
                                    bigquery.SchemaField("readAt",		"TIMESTAMP"),
                                    bigquery.SchemaField("repliedAt",		"TIMESTAMP"),
                                    bigquery.SchemaField("engagedAt",		"TIMESTAMP"),
                                    bigquery.SchemaField("usedPlainText",		"STRING"),
                                    bigquery.SchemaField("errorResolution",		"STRING"),
                                    bigquery.SchemaField("errorMessage",		"STRING"),
                                    bigquery.SchemaField("rawError",		"STRING"),
                                    bigquery.SchemaField("ipAddress",		"STRING"),
                                    bigquery.SchemaField("sentBy",		"STRING"),
                                    bigquery.SchemaField("name",		"STRING"),
                                    bigquery.SchemaField("workflowStatus",		"STRING"),
                                    bigquery.SchemaField("workflowNodeId",		"STRING"),
                                    bigquery.SchemaField("workflowUserId",		"STRING"),
                                    bigquery.SchemaField("quickReplyResponse",		"STRING"),
                                    bigquery.SchemaField("workflowDetails",		"STRING"),
                                    bigquery.SchemaField("notificationPostbackUrl",		"STRING"),
                                    bigquery.SchemaField("subSource",		"STRING"),
                                    bigquery.SchemaField("postbackAuthHeader",		"STRING"),
                                    bigquery.SchemaField("cdpUserId",		"STRING"),
                                    bigquery.SchemaField("smsUnits",		"STRING"),
                                    bigquery.SchemaField("sessionStart",		"STRING"),
                                    bigquery.SchemaField("sessionType",		"STRING"),
                                    bigquery.SchemaField("sessionConversationId",		"STRING"),
                                    bigquery.SchemaField("mdlzID","STRING"),
                                    bigquery.SchemaField("normalized_phone", "STRING")
                                    ]
                        total_rows_loaded = self.ya_bq_repository.load_table_from_gcs_to_bq(gcs_uri=gcs_uri, table_name=f"{self.config.gcp.gcp_bq_data_project_id}.{self.config.gcp.gcp_bq_dataset_id}.{self.config.gcp.gcp_bq_notification_table}", schema=schema)
                        if total_rows_loaded:
                            self.logger.info("Loaded %s rows for bot %s.", total_rows_loaded, bot_id)
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
                        'execution_id': str(uuid.uuid4()),
                        'process_name': f'in_ya_{self.config.app.notification_report_prefix}',
                        'source_name': str(self.config.gcp.gcp_gcs_data_bucket),
                        'load_type': 'File',
                        'load_source_type': 'GCS',
                        'source_detail': blob_info['name'],
                        'target_table': self.config.gcp.gcp_bq_notification_table,
                        'landing_time': blob_info['generation'],
                        'process_start_time': file_processing_start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_end_time': file_processing_end_time.strftime('%Y-%m-%dT%H:%M:%S'),
                        'process_status': file_load_status,
                        'rows_processed': total_rows_loaded,
                        'error_message': load_error_message,
                        'created_at': record_created_at,
                        'updated_at': datetime.now(pendulum.UTC).strftime('%Y-%m-%dT%H:%M:%S')
                    })

            if audit_rows:
                self.ya_bq_repository.insert_audit_records(audit_rows)
                processed_folder = 'processed_files/'
                processed_blobs = self.gcs_storage_client.bucket(self.config.gcp.gcp_gcs_data_bucket).list_blobs(prefix=processed_folder)
                for processed_blob in processed_blobs:
                    self.logger.info(f"Deleting processed blob: {processed_blob.name}")
                    processed_blob.delete()
                self.logger.info(f'Successfully deleted all the  processed blobs for the bucket')
            else:
                self.logger.info(f"No audit records to insert, skipping insert_audit_records.")
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error in load_data_to_bigquery: {e}")
            raise e
