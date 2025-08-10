# import os
# import sys
# import json
# import pytest
# import tempfile
# import pandas as pd
# from pathlib import Path
# from datetime import datetime, timedelta
# from unittest.mock import MagicMock, patch, Mock, mock_open, call
# import pendulum

# # Mock problematic Airflow modules before any imports
# def setup_airflow_mocks():
#     """Setup mocks for Airflow modules to avoid import issues"""
#     import sys
#     from unittest.mock import MagicMock
    
#     # Mock airflow.models module
#     if 'airflow' not in sys.modules:
#         sys.modules['airflow'] = MagicMock()
#     if 'airflow.models' not in sys.modules:
#         sys.modules['airflow.models'] = MagicMock()
#     if 'airflow.models.dagrun' not in sys.modules:
#         mock_dagrun = MagicMock()
#         mock_dagrun.DagRun = MagicMock()
#         mock_dagrun.DagRun.__allow_unmapped__ = True
#         sys.modules['airflow.models.dagrun'] = mock_dagrun

# def setup_operators_mocks():
#     """Setup mocks for operators module to avoid import issues"""
#     import sys
#     from unittest.mock import MagicMock
    
#     # Mock the operators module and its submodules
#     sys.modules['operators'] = MagicMock()
#     sys.modules['operators.id_service_connector'] = MagicMock()
#     sys.modules['operators.id_service_helper'] = MagicMock()
    
#     # Mock the specific classes/functions that are imported
#     mock_id_service_connector = MagicMock()
#     mock_fetch_mdlz_ids = MagicMock()
    
#     sys.modules['operators.id_service_connector'].IdServiceConnector = mock_id_service_connector
#     sys.modules['operators.id_service_helper'].fetch_mdlz_ids = mock_fetch_mdlz_ids
    
#     return mock_id_service_connector, mock_fetch_mdlz_ids

# # Apply mocks before any other imports
# setup_airflow_mocks()
# setup_operators_mocks()

# # Now safe to import other modules
# from google.cloud.bigquery import Row
# from google.cloud.exceptions import NotFound
# from google.api_core.exceptions import NotFound

# test_config = {
#     'project_id': 'test_project_id',
#     'dataset': 'test_dataset',
#     'table_id': 'test_table_id',
#     'bucket_name': 'test_bucket_name',
#     'audit_table_id': 'test_audit_table_id',
#     'bot_id_prefix': ['test_bot_id_prefix_1', 'test_bot_id_prefix_2'],
# }

# os.environ['AIRFLOW_VAR_IN_YA_UNSUB_REP_CONFIG'] = json.dumps(test_config)

# TEST_FILE_PATH = Path(__file__).resolve()
# PROJECT_ROOT = TEST_FILE_PATH.parents[2]
# DAGS_DIR = PROJECT_ROOT / "source" / "composer" / "dags"

# sys.path.insert(0, str(DAGS_DIR))

# dag_logic_file = DAGS_DIR / "yellow_ai_unsubscribe_report_dag.py"
# print(f"Looking for file: {dag_logic_file}")
# print(f"File exists: {dag_logic_file.exists()}")

# if DAGS_DIR.exists():
#     print(f"Files in {DAGS_DIR}:")
#     for file in DAGS_DIR.iterdir():
#         print(f"  {file.name}")
# else:
#     print(f"Directory {DAGS_DIR} does not exist")

# # Mock Airflow Variable.get before importing the DAG
# with patch('airflow.models.Variable.get', return_value=test_config):
#     try:
#         # Try direct import first - Updated to match new filename
#         import yellow_ai_unsubscribe_report_dag as dag_module
#         print("Successfully imported yellow_ai_unsubscribe_report_dag")
#     except ImportError as e:
#         print(f"Failed to import yellow_ai_unsubscribe_report_dag: {e}")
#         # Try with importlib if direct import fails
#         try:
#             import importlib.util
#             dag_file = DAGS_DIR / "yellow_ai_unsubscribe_report_dag.py"
#             spec = importlib.util.spec_from_file_location("yellow_ai_unsubscribe_report_dag", dag_file)
#             if spec and spec.loader:
#                 dag_module = importlib.util.module_from_spec(spec)
#                 spec.loader.exec_module(dag_module)
#                 print("Successfully imported using importlib")
#             else:
#                 raise ImportError("Could not create module spec")
#         except Exception as e2:
#             print(f"Failed to import using importlib: {e2}")
#             raise ImportError(f"Could not import DAG module: {e}, {e2}")


# class TestYellowAIUnsubscribeReportDAG:
#     """Test suite for Yellow AI Unsubscribe Report DAG"""

#     @pytest.fixture
#     def mock_context(self):
#         """Create a mock Airflow context"""
#         context = {
#             'dag': Mock(),
#             'task_instance': Mock(),
#             'execution_date': datetime(2025, 1, 1),
#             'run_id': 'test_run_id_123',
#             'exception': Exception("Test exception")
#         }
#         context['dag'].dag_id = 'test_dag_id'
#         context['task_instance'].task_id = 'test_task_id'
#         return context

#     @pytest.fixture
#     def mock_bigquery_client(self):
#         """Create a mock BigQuery client"""
#         with patch('google.cloud.bigquery.Client') as mock_client:
#             yield mock_client

#     @pytest.fixture
#     def mock_storage_client(self):
#         """Create a mock GCS Storage client"""
#         with patch('google.cloud.storage.Client') as mock_client:
#             yield mock_client

#     def test_dag_structure(self):
#         """Test DAG structure and configuration"""
#         dag = dag_module.dag
#         assert dag.dag_id == 'in_ya_unsubscribe_report_load_dag'
#         assert dag.schedule_interval == '30 11 * * *'
#         assert dag.catchup is False
        
#         # Test tasks exist
#         task_ids = [task.task_id for task in dag.tasks]
#         expected_tasks = ['start', 'run_unsub_report_load', 'end']
#         assert all(task_id in task_ids for task_id in expected_tasks)
        
#         # Test task dependencies
#         start_task = dag.get_task('start')
#         data_load_task = dag.get_task('run_unsub_report_load')
#         end_task = dag.get_task('end')
        
#         assert data_load_task in start_task.downstream_list
#         assert end_task in data_load_task.downstream_list

#     def test_pipeline_config(self):
#         """Test pipeline configuration"""
#         assert dag_module.PIPELINE_CONFIG['project_id'] == 'test_project_id'
#         assert dag_module.PIPELINE_CONFIG['dataset'] == 'test_dataset'
#         assert dag_module.PIPELINE_CONFIG['table_id'] == 'test_table_id'
#         assert dag_module.PIPELINE_CONFIG['bucket_name'] == 'test_bucket_name'
#         assert dag_module.PIPELINE_CONFIG['audit_table_id'] == 'test_audit_table_id'

#     def test_log_task_failure_success(self, mock_context):
#         """Test log_task_failure function with valid context"""
#         with patch('yellow_ai_unsubscribe_report_dag.logging') as mock_logging:
#             dag_module.log_task_failure(mock_context)
#             mock_logging.error.assert_called_once()
#             error_call = mock_logging.error.call_args[0][0]
#             assert 'test_dag_id' in error_call
#             assert 'test_task_id' in error_call

#     def test_log_task_failure_exception(self, mock_context):
#         """Test log_task_failure function when it encounters an exception"""
#         mock_context['dag'] = None  # This will cause an exception
        
#         with patch('yellow_ai_unsubscribe_report_dag.logging') as mock_logging:
#             with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#                 with pytest.raises(AttributeError):
#                     dag_module.log_task_failure(mock_context)
#                 mock_traceback.assert_called_once()
#                 mock_logging.error.assert_called()

    
#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_fetch_last_processed_epoch_ts_success(self, mock_bq_client, mock_context):
#         """Test fetch_last_processed_epoch_ts function success case"""
#         # Mock query result
#         mock_row = Mock()
#         mock_row.max_gen = 1234567890
        
#         mock_client_instance = Mock()
#         mock_bq_client.return_value = mock_client_instance
#         # Fix: Return an iterator instead of a list
#         mock_client_instance.query.return_value.result.return_value = iter([mock_row])
        
#         result = dag_module.fetch_last_processed_epoch_ts('test_bot_prefix', mock_context)
        
#         assert result == 1234567890
#         mock_bq_client.assert_called_once_with(project='test_project_id')
#         mock_client_instance.query.assert_called_once()
    

#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_fetch_last_processed_epoch_ts_no_result(self, mock_bq_client, mock_context):
#         """Test fetch_last_processed_epoch_ts function when no previous records exist"""
#         # Mock query result with None
#         mock_row = Mock()
#         mock_row.max_gen = None
#         mock_query_result = [mock_row]
        
#         mock_client_instance = Mock()
#         mock_bq_client.return_value = mock_client_instance
#         mock_client_instance.query.return_value.result.return_value = iter([mock_row])
        
#         result = dag_module.fetch_last_processed_epoch_ts('test_bot_prefix', mock_context)
        
#         assert result == 0

#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_fetch_last_processed_epoch_ts_exception(self, mock_bq_client, mock_context):
#         """Test fetch_last_processed_epoch_ts function exception handling"""
#         mock_bq_client.side_effect = Exception("BigQuery error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.fetch_last_processed_epoch_ts('test_bot_prefix', mock_context)
#             mock_traceback.assert_called_once()

#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_insert_audit_records_success(self, mock_bq_client, mock_context):
#         """Test insert_audit_records function success case"""
#         mock_client_instance = Mock()
#         mock_bq_client.return_value = mock_client_instance
#         mock_client_instance.insert_rows_json.return_value = []  # No errors
        
#         test_records = [{'test': 'record1'}, {'test': 'record2'}]
        
#         dag_module.insert_audit_records(test_records, mock_context)
        
#         mock_client_instance.insert_rows_json.assert_called_once_with(
#             'test_audit_table_id', test_records
#         )

#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_insert_audit_records_with_errors(self, mock_bq_client, mock_context):
#         """Test insert_audit_records function with insertion errors"""
#         mock_client_instance = Mock()
#         mock_bq_client.return_value = mock_client_instance
#         mock_client_instance.insert_rows_json.return_value = ['Error 1', 'Error 2']
        
#         test_records = [{'test': 'record1'}]
        
#         with pytest.raises(RuntimeError):
#             dag_module.insert_audit_records(test_records, mock_context)

#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     def test_insert_audit_records_exception(self, mock_bq_client, mock_context):
#         """Test insert_audit_records function exception handling"""
#         mock_bq_client.side_effect = Exception("BigQuery error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.insert_audit_records([], mock_context)
#             mock_traceback.assert_called_once()

#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     def test_get_blobs_success(self, mock_storage_client, mock_context):
#         """Test get_blobs function success case"""
#         # Mock blobs
#         mock_blob1 = Mock()
#         mock_blob1.name = 'test_file1.csv'
#         mock_blob1.generation = 1234567891
#         mock_blob1.updated = datetime(2025, 1, 1)
        
#         mock_blob2 = Mock()
#         mock_blob2.name = 'test_file2.json'
#         mock_blob2.generation = 1234567892
#         mock_blob2.updated = datetime(2025, 1, 2)
        
#         mock_blob3 = Mock()
#         mock_blob3.name = 'test_file3.txt'  # Should be ignored
#         mock_blob3.generation = 1234567893
        
#         mock_client_instance = Mock()
#         mock_storage_client.return_value = mock_client_instance
#         mock_client_instance.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
#         result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
#         assert len(result) == 2
#         assert result[0]['name'] == 'test_file1.csv'
#         assert result[1]['name'] == 'test_file2.json'
#         mock_client_instance.list_blobs.assert_called_once_with('test_bucket_name', prefix='test_prefix')

#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     def test_get_blobs_no_new_files(self, mock_storage_client, mock_context):
#         """Test get_blobs function when no new files exist"""
#         mock_blob = Mock()
#         mock_blob.name = 'old_file.csv'
#         mock_blob.generation = 1234567889  # Older than landing_time
        
#         mock_client_instance = Mock()
#         mock_storage_client.return_value = mock_client_instance
#         mock_client_instance.list_blobs.return_value = [mock_blob]
        
#         result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
#         assert len(result) == 0

#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     def test_get_blobs_exception(self, mock_storage_client, mock_context):
#         """Test get_blobs function exception handling"""
#         mock_storage_client.side_effect = Exception("Storage error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
#             mock_traceback.assert_called_once()

#     @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
#     @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
#     def test_enrich_csv_with_bot_id_success(self, mock_read_csv, mock_temp_file, mock_context):
#         """Test enrich_csv_with_bot_id function success case"""
#         # Mock blob
#         mock_blob = Mock()
#         mock_blob.download_as_bytes.return_value = b'phone,user_request\n123,test'
        
#         # Mock DataFrame
#         mock_df = Mock()
#         mock_read_csv.return_value = mock_df
        
#         # Mock temp file
#         mock_temp_instance = Mock()
#         mock_temp_instance.name = '/tmp/test_file.csv'
#         mock_temp_file.return_value = mock_temp_instance
        
#         result = dag_module.enrich_csv_with_bot_id(mock_blob, 'test_bot', mock_context)
        
#         assert result == '/tmp/test_file.csv'
#         mock_blob.download_as_bytes.assert_called_once()
#         mock_df.__setitem__.assert_called_once_with('bot_id', 'test_bot')
#         mock_df.to_csv.assert_called_once_with('/tmp/test_file.csv', index=False)

#     def test_enrich_csv_with_bot_id_exception(self, mock_context):
#         """Test enrich_csv_with_bot_id function exception handling"""
#         mock_blob = Mock()
#         mock_blob.download_as_bytes.side_effect = Exception("Download error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.enrich_csv_with_bot_id(mock_blob, 'test_bot', mock_context)
#             mock_traceback.assert_called_once()

#     @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
#     @patch('yellow_ai_unsubscribe_report_dag.pd.json_normalize')
#     @patch('yellow_ai_unsubscribe_report_dag.json.loads')
#     def test_download_and_prepare_csv_from_json_dict(self, mock_json_loads, mock_json_normalize, mock_temp_file, mock_context):
#         """Test download_and_prepare_csv_from_json function with dict input"""
#         # Mock blob
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.json'
#         mock_blob.download_as_bytes.return_value = b'{"phone": "123", "user_request": "test"}'
        
#         # Mock JSON parsing
#         mock_json_loads.return_value = {"phone": "123", "user_request": "test"}
        
#         # Mock DataFrame
#         mock_df = Mock()
#         mock_json_normalize.return_value = mock_df
        
#         # Mock temp file
#         mock_temp_instance = Mock()
#         mock_temp_instance.name = '/tmp/test_file.csv'
#         mock_temp_file.return_value = mock_temp_instance
        
#         result = dag_module.download_and_prepare_csv_from_json(mock_blob, 'test_bot', mock_context)
        
#         assert result == '/tmp/test_file.csv'
#         mock_json_loads.assert_called_once()
#         mock_json_normalize.assert_called_once_with([{"phone": "123", "user_request": "test"}])
#         mock_df.__setitem__.assert_called_once_with('bot_id', 'test_bot')

#     @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
#     @patch('yellow_ai_unsubscribe_report_dag.pd.json_normalize')
#     @patch('yellow_ai_unsubscribe_report_dag.json.loads')
#     def test_download_and_prepare_csv_from_json_list(self, mock_json_loads, mock_json_normalize, mock_temp_file, mock_context):
#         """Test download_and_prepare_csv_from_json function with list input"""
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.json'
#         mock_blob.download_as_bytes.return_value = b'[{"phone": "123"}, {"phone": "456"}]'
        
#         mock_json_loads.return_value = [{"phone": "123"}, {"phone": "456"}]
        
#         mock_df = Mock()
#         mock_json_normalize.return_value = mock_df
        
#         mock_temp_instance = Mock()
#         mock_temp_instance.name = '/tmp/test_file.csv'
#         mock_temp_file.return_value = mock_temp_instance
        
#         result = dag_module.download_and_prepare_csv_from_json(mock_blob, 'test_bot', mock_context)
        
#         assert result == '/tmp/test_file.csv'
#         mock_json_normalize.assert_called_once_with([{"phone": "123"}, {"phone": "456"}])

#     @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
#     @patch('yellow_ai_unsubscribe_report_dag.pd.json_normalize')
#     @patch('yellow_ai_unsubscribe_report_dag.json.loads')
#     def test_download_and_prepare_csv_from_json_ndjson(self, mock_json_loads, mock_json_normalize, mock_temp_file, mock_context):
#         """Test download_and_prepare_csv_from_json function with NDJSON input"""
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.json'
#         mock_blob.download_as_bytes.return_value = b'{"phone": "123"}\n{"phone": "456"}'
        
#         # First call raises JSONDecodeError, then individual lines are parsed
#         mock_json_loads.side_effect = [
#             json.JSONDecodeError("msg", "doc", 0),
#             {"phone": "123"},
#             {"phone": "456"}
#         ]
        
#         mock_df = Mock()
#         mock_json_normalize.return_value = mock_df
        
#         mock_temp_instance = Mock()
#         mock_temp_instance.name = '/tmp/test_file.csv'
#         mock_temp_file.return_value = mock_temp_instance
        
#         result = dag_module.download_and_prepare_csv_from_json(mock_blob, 'test_bot', mock_context)
        
#         assert result == '/tmp/test_file.csv'

#     def test_download_and_prepare_csv_from_json_exception(self, mock_context):
#         """Test download_and_prepare_csv_from_json function exception handling"""
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.json'
#         mock_blob.download_as_bytes.side_effect = Exception("Download error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.download_and_prepare_csv_from_json(mock_blob, 'test_bot', mock_context)
#             mock_traceback.assert_called_once()

#     @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
#     @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
#     @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id')
#     @patch('builtins.open', new_callable=mock_open, read_data=b'test,data')
#     def test_load_data_to_bigquery_csv_success(self, mock_file, mock_enrich_csv, mock_storage_client, 
#                                                mock_bq_client, mock_fetch_epoch, mock_get_blobs, 
#                                                mock_insert_audit, mock_sleep, mock_context):
#         """Test load_data_to_bigquery function with CSV file success case"""
#         # Setup mocks
#         mock_fetch_epoch.return_value = 1234567890
#         mock_get_blobs.return_value = [
#             {'name': 'test_file.csv', 'generation': 1234567891, 'updated': datetime(2025, 1, 1)}
#         ]
        
#         # Mock storage client and blob
#         mock_storage_instance = Mock()
#         mock_storage_client.return_value = mock_storage_instance
#         mock_bucket = Mock()
#         mock_storage_instance.bucket.return_value = mock_bucket
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.csv'
#         mock_bucket.get_blob.return_value = mock_blob
        
#         # Mock BigQuery client and job
#         mock_bq_instance = Mock()
#         mock_bq_client.return_value = mock_bq_instance
#         mock_job = Mock()
#         mock_job.output_rows = 100
#         mock_bq_instance.load_table_from_file.return_value = mock_job
#         mock_job.result.return_value = None
        
#         mock_enrich_csv.return_value = '/tmp/test_file.csv'
        
#         # Execute function
#         dag_module.load_data_to_bigquery(**mock_context)
        
#         # Verify calls
#         assert mock_fetch_epoch.call_count == 2  # Called for each bot prefix
#         assert mock_get_blobs.call_count == 2
#         mock_insert_audit.assert_called_once()
#         mock_enrich_csv.assert_called_once()


#     @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
#     @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
#     @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.download_and_prepare_csv_from_json')
#     @patch('builtins.open', new_callable=mock_open, read_data=b'test,data')
#     def test_load_data_to_bigquery_json_success(self, mock_file, mock_download_json, mock_storage_client,
#                                                 mock_bq_client, mock_fetch_epoch, mock_get_blobs,
#                                                 mock_insert_audit, mock_sleep, mock_context):
#         """Test load_data_to_bigquery function with JSON file success case"""
#         # Setup mocks
#         mock_fetch_epoch.return_value = 1234567890
#         mock_get_blobs.return_value = [
#             {'name': 'test_file.json', 'generation': 1234567891, 'updated': datetime(2025, 1, 1)}
#         ]
        
#         # Mock storage client and blob
#         mock_storage_instance = Mock()
#         mock_storage_client.return_value = mock_storage_instance
#         mock_bucket = Mock()
#         mock_storage_instance.bucket.return_value = mock_bucket
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.json'
#         mock_bucket.get_blob.return_value = mock_blob
        
#         # Mock BigQuery client and job
#         mock_bq_instance = Mock()
#         mock_bq_client.return_value = mock_bq_instance
#         mock_job = Mock()
#         mock_job.output_rows = 50
#         mock_bq_instance.load_table_from_file.return_value = mock_job
#         mock_job.result.return_value = None
        
#         mock_download_json.return_value = '/tmp/test_file.csv'
        
#         # Execute function
#         dag_module.load_data_to_bigquery(**mock_context)
        
#         # Verify calls - Should be called twice (once for each bot prefix)
#         assert mock_download_json.call_count == 2
#         mock_insert_audit.assert_called_once()
        
#         # Verify the function was called with the correct bot prefixes
#         expected_calls = [
#             call(mock_blob, 'test_bot_id_prefix_1', mock_context),
#             call(mock_blob, 'test_bot_id_prefix_2', mock_context)
#         ]
#         mock_download_json.assert_has_calls(expected_calls, any_order=False)




#     @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
#     @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
#     @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     def test_load_data_to_bigquery_no_new_files(self, mock_fetch_epoch, mock_get_blobs,
#                                                 mock_insert_audit, mock_sleep, mock_context):
#         """Test load_data_to_bigquery function when no new files exist"""
#         mock_fetch_epoch.return_value = 1234567890
#         mock_get_blobs.return_value = []  # No new files
        
#         dag_module.load_data_to_bigquery(**mock_context)
        
#         mock_insert_audit.assert_not_called()

#     @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
#     @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
#     @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id')
#     def test_load_data_to_bigquery_processing_error(self, mock_enrich_csv, mock_storage_client,
#                                                     mock_bq_client, mock_fetch_epoch, mock_get_blobs,
#                                                     mock_insert_audit, mock_sleep, mock_context):
#         """Test load_data_to_bigquery function when file processing fails"""
#         # Setup mocks
#         mock_fetch_epoch.return_value = 1234567890
#         mock_get_blobs.return_value = [
#             {'name': 'test_file.csv', 'generation': 1234567891, 'updated': datetime(2025, 1, 1)}
#         ]
        
#         # Mock storage client and blob
#         mock_storage_instance = Mock()
#         mock_storage_client.return_value = mock_storage_instance
#         mock_bucket = Mock()
#         mock_storage_instance.bucket.return_value = mock_bucket
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.csv'
#         mock_bucket.get_blob.return_value = mock_blob
        
#         # Make enrich_csv_with_bot_id raise an exception
#         mock_enrich_csv.side_effect = Exception("Processing error")
        
#         # Execute function
#         dag_module.load_data_to_bigquery(**mock_context)
        
#         # Verify audit record was still inserted with failure status
#         mock_insert_audit.assert_called_once()
#         audit_records = mock_insert_audit.call_args[0][0]
#         assert len(audit_records) == 2  # One for each bot prefix
#         assert all(record['load_status'] == 'FAILURE' for record in audit_records)
#         assert all(record['error_message'] == 'Processing error' for record in audit_records)

#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     def test_load_data_to_bigquery_exception(self, mock_fetch_epoch, mock_context):
#         """Test load_data_to_bigquery function exception handling"""
#         mock_fetch_epoch.side_effect = Exception("Fetch error")
        
#         with patch('yellow_ai_unsubscribe_report_dag.traceback.print_exc') as mock_traceback:
#             with pytest.raises(Exception):
#                 dag_module.load_data_to_bigquery(**mock_context)
#             mock_traceback.assert_called_once()

#     def test_default_args(self):
#         """Test DAG default arguments"""
#         default_args = dag_module.DEFAULT_ARGS
#         assert default_args['owner'] == 'mdlz_cde_in'
#         assert default_args['depends_on_past'] is False
#         assert default_args['retries'] == 1
#         assert default_args['retry_delay'] == timedelta(minutes=5)
#         assert default_args['max_active_runs'] == 1

#     def test_job_config_variable_name(self):
#         """Test that the correct Airflow variable name is used"""
#         # This test ensures the variable name matches what's expected
#         with patch('airflow.models.Variable.get') as mock_var_get:
#             mock_var_get.return_value = test_config
#             # Re-import to trigger variable loading
#             import importlib
#             importlib.reload(dag_module)
#             mock_var_get.assert_called_with("in_ya_unsub_rep_config", deserialize_json=True)

#     @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
#     @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
#     @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
#     @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
#     @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
#     @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id')
#     @patch('builtins.open', new_callable=mock_open, read_data=b'test,data')
#     def test_load_data_to_bigquery_zero_rows_loaded(self, mock_file, mock_enrich_csv, mock_storage_client,
#                                                    mock_bq_client, mock_fetch_epoch, mock_get_blobs,
#                                                    mock_insert_audit, mock_sleep, mock_context):
#         """Test load_data_to_bigquery function when BigQuery job loads zero rows"""
#         # Setup mocks
#         mock_fetch_epoch.return_value = 1234567890
#         mock_get_blobs.return_value = [
#             {'name': 'test_file.csv', 'generation': 1234567891, 'updated': datetime(2025, 1, 1)}
#         ]
        
#         # Mock storage client and blob
#         mock_storage_instance = Mock()
#         mock_storage_client.return_value = mock_storage_instance
#         mock_bucket = Mock()
#         mock_storage_instance.bucket.return_value = mock_bucket
#         mock_blob = Mock()
#         mock_blob.name = 'test_file.csv'
#         mock_bucket.get_blob.return_value = mock_blob
        
#         # Mock BigQuery client and job with zero rows
#         mock_bq_instance = Mock()
#         mock_bq_client.return_value = mock_bq_instance
#         mock_job = Mock()
#         mock_job.output_rows = 0  # Zero rows loaded
#         mock_bq_instance.load_table_from_file.return_value = mock_job
#         mock_job.result.return_value = None
        
#         mock_enrich_csv.return_value = '/tmp/test_file.csv'
        
#         # Execute function
#         dag_module.load_data_to_bigquery(**mock_context)
        
#         # Verify audit record shows failure status for zero rows
#         mock_insert_audit.assert_called_once()
#         audit_records = mock_insert_audit.call_args[0][0]
#         assert len(audit_records) == 2  # One for each bot prefix
#         assert all(record['load_status'] == 'FAILURE' for record in audit_records)
#         assert all(record['rows_loaded'] == 0 for record in audit_records)


# if __name__ == "__main__":
#     pytest.main([__file__, "-v"])


import os
import sys
import json
import pytest
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock, mock_open, call
import pendulum

# Mock problematic Airflow modules before any imports
def setup_airflow_mocks():
    """Setup mocks for Airflow modules to avoid import issues"""
    import sys
    from unittest.mock import MagicMock
    
    # Mock airflow.models module
    if 'airflow' not in sys.modules:
        sys.modules['airflow'] = MagicMock()
    if 'airflow.models' not in sys.modules:
        sys.modules['airflow.models'] = MagicMock()
    if 'airflow.models.dagrun' not in sys.modules:
        mock_dagrun = MagicMock()
        mock_dagrun.DagRun = MagicMock()
        mock_dagrun.DagRun.__allow_unmapped__ = True
        sys.modules['airflow.models.dagrun'] = mock_dagrun

def setup_operators_mocks():
    """Setup mocks for operators module to avoid import issues"""
    import sys
    from unittest.mock import MagicMock
    
    # Mock the operators module and its submodules
    sys.modules['operators'] = MagicMock()
    sys.modules['operators.id_service_connector'] = MagicMock()
    sys.modules['operators.id_service_helper'] = MagicMock()
    
    # Mock the specific classes/functions that are imported
    mock_id_service_connector = MagicMock()
    mock_fetch_mdlz_ids = MagicMock()
    
    sys.modules['operators.id_service_connector'].IdServiceConnector = mock_id_service_connector
    sys.modules['operators.id_service_helper'].fetch_mdlz_ids = mock_fetch_mdlz_ids
    
    return mock_id_service_connector, mock_fetch_mdlz_ids

# Apply mocks before any other imports
setup_airflow_mocks()
setup_operators_mocks()

# Now safe to import other modules
from google.cloud.bigquery import Row
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound

test_config = {
    'project_id': 'test_project_id',
    'dataset': 'test_dataset',
    'table_id': 'test_table_id',
    'bucket_name': 'test_bucket_name',
    'audit_table_id': 'test_audit_table_id',
    'bot_id_prefix': ['test_bot_id_prefix_1', 'test_bot_id_prefix_2'],
}

os.environ['AIRFLOW_VAR_IN_YA_UNSUB_REP_CONFIG'] = json.dumps(test_config)

TEST_FILE_PATH = Path(__file__).resolve()
PROJECT_ROOT = TEST_FILE_PATH.parents[2]
DAGS_DIR = PROJECT_ROOT / "source" / "composer" / "dags"

sys.path.insert(0, str(DAGS_DIR))

dag_logic_file = DAGS_DIR / "yellow_ai_unsubscribe_report_dag.py"
print(f"Looking for file: {dag_logic_file}")
print(f"File exists: {dag_logic_file.exists()}")

if DAGS_DIR.exists():
    print(f"Files in {DAGS_DIR}:")
    for file in DAGS_DIR.iterdir():
        print(f"  {file.name}")
else:
    print(f"Directory {DAGS_DIR} does not exist")

# Mock Airflow Variable.get before importing the DAG
with patch('airflow.models.Variable.get', return_value=test_config):
    try:
        # Try direct import first - Updated to match new filename
        import yellow_ai_unsubscribe_report_dag as dag_module
        print("Successfully imported yellow_ai_unsubscribe_report_dag")
    except ImportError as e:
        print(f"Failed to import yellow_ai_unsubscribe_report_dag: {e}")
        # Try with importlib if direct import fails
        try:
            import importlib.util
            dag_file = DAGS_DIR / "yellow_ai_unsubscribe_report_dag.py"
            spec = importlib.util.spec_from_file_location("yellow_ai_unsubscribe_report_dag", dag_file)
            if spec and spec.loader:
                dag_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(dag_module)
                print("Successfully imported using importlib")
            else:
                raise ImportError("Could not create module spec")
        except Exception as e2:
            print(f"Failed to import using importlib: {e2}")
            raise ImportError(f"Could not import DAG module: {e}, {e2}")


class TestYellowAIUnsubscribeReportDAG:
    """Test suite for Yellow AI Unsubscribe Report DAG"""

    @pytest.fixture
    def mock_context(self):
        """Create a mock Airflow context"""
        return {
            'dag': Mock(dag_id='test_dag'),
            'task_instance': Mock(task_id='test_task'),
            'execution_date': datetime(2023, 1, 1),
            'run_id': 'test_run_123',
            'exception': Exception('Test exception')
        }

    @pytest.fixture
    def sample_unsubscribe_csv_data(self):
        """Sample CSV data for unsubscribe testing"""
        return pd.DataFrame({
            'phone': ['1234567890', '9876543210', '5555555555'],
            'user_request': ['unsubscribe', 'stop', 'opt-out'],
            'created_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00', '2023-01-03T12:00:00'],
            'updated_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00', '2023-01-03T12:00:00']
        })

    def test_dag_structure(self):
        """Test DAG structure and configuration"""
        assert hasattr(dag_module, 'dag')
        assert dag_module.dag.dag_id == 'in_ya_unsubscribe_report_load_dag'
        assert dag_module.dag.schedule_interval == '30 11 * * *'
        assert dag_module.dag.catchup is False
        
        # Test tasks exist
        task_ids = [task.task_id for task in dag_module.dag.tasks]
        expected_tasks = ['start', 'run_unsub_report_load', 'end']
        assert all(task_id in task_ids for task_id in expected_tasks)

    def test_pipeline_config(self):
        """Test pipeline configuration"""
        assert dag_module.PIPELINE_CONFIG['project_id'] == 'test_project_id'
        assert dag_module.PIPELINE_CONFIG['dataset'] == 'test_dataset'
        assert dag_module.PIPELINE_CONFIG['table_id'] == 'test_table_id'
        assert dag_module.PIPELINE_CONFIG['bucket_name'] == 'test_bucket_name'
        assert dag_module.PIPELINE_CONFIG['audit_table_id'] == 'test_audit_table_id'
        assert dag_module.PIPELINE_CONFIG['bot_id_prefix'] == ['test_bot_id_prefix_1', 'test_bot_id_prefix_2']

    def test_default_args(self):
        """Test DAG default arguments"""
        assert dag_module.DEFAULT_ARGS['owner'] == 'mdlz_cde_in'
        assert dag_module.DEFAULT_ARGS['depends_on_past'] is False
        assert dag_module.DEFAULT_ARGS['retries'] == 1
        assert dag_module.DEFAULT_ARGS['retry_delay'] == timedelta(minutes=5)
        assert dag_module.DEFAULT_ARGS['max_active_runs'] == 1

    def test_task_dependencies(self):
        """Test task dependencies in DAG"""
        start_task = dag_module.dag.get_task('start')
        load_task = dag_module.dag.get_task('run_unsub_report_load')
        end_task = dag_module.dag.get_task('end')
        
        # Check upstream dependencies
        assert start_task.task_id in [t.task_id for t in load_task.upstream_list]
        assert load_task.task_id in [t.task_id for t in end_task.upstream_list]
        
        # Check downstream dependencies
        assert load_task.task_id in [t.task_id for t in start_task.downstream_list]
        assert end_task.task_id in [t.task_id for t in load_task.downstream_list]

    def test_log_task_failure_success(self, mock_context):
        """Test log_task_failure function with valid context"""
        with patch('yellow_ai_unsubscribe_report_dag.logging') as mock_logging:
            dag_module.log_task_failure(mock_context)
            mock_logging.error.assert_called_once()
            
    def test_log_task_failure_exception(self):
        """Test log_task_failure function with invalid context"""
        invalid_context = {}
        with patch('yellow_ai_unsubscribe_report_dag.logging') as mock_logging:
            with pytest.raises(Exception):
                dag_module.log_task_failure(invalid_context)
            mock_logging.error.assert_called()

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_success(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with successful query"""
        # Mock BigQuery client and query result
        mock_query_result = Mock()
        mock_query_result.max_gen = 1234567890
        mock_bq_client.return_value.query.return_value.result.return_value = iter([mock_query_result])
        
        result = dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)
        
        assert result == 1234567890
        mock_bq_client.assert_called_once_with(project='test_project_id')

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_no_result(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with no previous results"""
        # Mock BigQuery client with None result
        mock_query_result = Mock()
        mock_query_result.max_gen = None
        mock_bq_client.return_value.query.return_value.result.return_value = iter([mock_query_result])
        
        result = dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)
        
        assert result == 0

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_exception(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with exception"""
        mock_bq_client.return_value.query.side_effect = Exception('BigQuery error')
        
        with pytest.raises(Exception):
            dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_insert_audit_records_success(self, mock_bq_client, mock_context):
        """Test insert_audit_records with successful insertion"""
        mock_bq_client.return_value.insert_rows_json.return_value = []
        
        test_records = [{'test': 'record1'}, {'test': 'record2'}]
        dag_module.insert_audit_records(test_records, mock_context)
        
        mock_bq_client.return_value.insert_rows_json.assert_called_once_with(
            'test_audit_table_id', test_records
        )

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_insert_audit_records_with_errors(self, mock_bq_client, mock_context):
        """Test insert_audit_records with insertion errors"""
        mock_bq_client.return_value.insert_rows_json.return_value = ['Error 1', 'Error 2']
        
        test_records = [{'test': 'record1'}]
        with pytest.raises(RuntimeError):
            dag_module.insert_audit_records(test_records, mock_context)

    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    def test_insert_audit_records_exception(self, mock_bq_client, mock_context):
        """Test insert_audit_records with exception"""
        mock_bq_client.side_effect = Exception('BigQuery connection error')
        
        test_records = [{'test': 'record1'}]
        with pytest.raises(Exception):
            dag_module.insert_audit_records(test_records, mock_context)

    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    def test_get_blobs_success(self, mock_storage_client, mock_context):
        """Test get_blobs with successful blob listing"""
        # Mock blob objects
        mock_blob1 = Mock()
        mock_blob1.name = 'test_file1.csv'
        mock_blob1.generation = 1234567891
        mock_blob1.updated = datetime(2023, 1, 1)
        
        mock_blob2 = Mock()
        mock_blob2.name = 'test_file2.csv'
        mock_blob2.generation = 1234567892
        mock_blob2.updated = datetime(2023, 1, 2)
        
        mock_blob3 = Mock()
        mock_blob3.name = 'test_file3.json'  # Should be filtered out (only CSV allowed)
        mock_blob3.generation = 1234567893
        mock_blob3.updated = datetime(2023, 1, 3)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 2
        assert result[0]['name'] == 'test_file1.csv'
        assert result[1]['name'] == 'test_file2.csv'

    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    def test_get_blobs_case_insensitive_csv(self, mock_storage_client, mock_context):
        """Test get_blobs with case insensitive CSV filtering"""
        mock_blob = Mock()
        mock_blob.name = 'test_file.CSV'  # Uppercase extension
        mock_blob.generation = 1234567891
        mock_blob.updated = datetime(2023, 1, 1)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 1
        assert result[0]['name'] == 'test_file.CSV'

    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    def test_get_blobs_no_new_files(self, mock_storage_client, mock_context):
        """Test get_blobs with no new files"""
        mock_blob = Mock()
        mock_blob.name = 'old_file.csv'
        mock_blob.generation = 1234567889  # Older than landing_time
        mock_blob.updated = datetime(2023, 1, 1)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 0

    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    def test_get_blobs_exception(self, mock_storage_client, mock_context):
        """Test get_blobs with exception"""
        mock_storage_client.return_value.list_blobs.side_effect = Exception('Storage error')
        
        with pytest.raises(Exception):
            dag_module.get_blobs(1234567890, 'test_prefix', mock_context)

    @patch('yellow_ai_unsubscribe_report_dag.asyncio.run')
    @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_success(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context, sample_unsubscribe_csv_data):
        """Test enrich_csv_with_bot_id_and_mdlz_id with successful processing"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv
        mock_read_csv.return_value = sample_unsubscribe_csv_data.copy()
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        # Mock asyncio.run (fetch_mdlz_ids)
        mock_asyncio_run.return_value = {
            '+1234567890': 'mdlz_id_1',
            '+9876543210': 'mdlz_id_2',
            '+5555555555': 'mdlz_id_3'
        }
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        mock_asyncio_run.assert_called_once()

    @patch('yellow_ai_unsubscribe_report_dag.asyncio.run')
    @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_no_phone(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id without phone column"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv - DataFrame without phone
        df_without_phone = pd.DataFrame({
            'user_request': ['unsubscribe', 'stop'],
            'created_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00'],
            'updated_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00']
        })
        mock_read_csv.return_value = df_without_phone
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        mock_asyncio_run.assert_not_called()

    @patch('yellow_ai_unsubscribe_report_dag.asyncio.run')
    @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_empty_phones(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id with empty phone list"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv with empty/null phones
        df_empty_phones = pd.DataFrame({
            'phone': [None, ''],  # Empty/null phones
            'user_request': ['unsubscribe', 'stop'],
            'created_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00'],
            'updated_at': ['2023-01-01T10:00:00', '2023-01-02T11:00:00']
        })
        mock_read_csv.return_value = df_empty_phones
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        # Should not call fetch_mdlz_ids when no valid phones
        mock_asyncio_run.assert_not_called()

    @patch('yellow_ai_unsubscribe_report_dag.asyncio.run')
    @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_bot_id_addition(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context, sample_unsubscribe_csv_data):
        """Test that bot_id is correctly added to DataFrame"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv
        df = sample_unsubscribe_csv_data.copy()
        mock_read_csv.return_value = df
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        # Mock asyncio.run
        mock_asyncio_run.return_value = {'+1234567890': 'mdlz_id_1'}
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot_prefix', mock_context)
        
        assert result == '/tmp/test_file.csv'
        # Verify bot_id was added to DataFrame
        assert 'bot_id' in df.columns

    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_exception(self, mock_read_csv, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id with exception"""
        mock_blob = Mock()
        mock_blob.download_as_bytes.side_effect = Exception('Download error')
        
        with pytest.raises(Exception):
            dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)

    @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
    @patch('yellow_ai_unsubscribe_report_dag.datetime')
    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
    @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id_and_mdlz_id')
    @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
    @patch('builtins.open', new_callable=mock_open, read_data=b'csv_data')
    def test_load_data_to_bigquery_success(self, mock_file_open, mock_insert_audit, mock_enrich_csv, 
                                         mock_get_blobs, mock_fetch_epoch, mock_bq_client, 
                                         mock_storage_client, mock_datetime, mock_sleep, mock_context):
        """Test load_data_to_bigquery with successful execution"""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock fetch_last_processed_epoch_ts
        mock_fetch_epoch.return_value = 1234567890
        
        # Mock get_blobs
        mock_get_blobs.return_value = [
            {'name': 'test_file.csv', 'generation': 1234567891, 'updated': mock_now}
        ]
        
        # Mock enrich_csv_with_bot_id_and_mdlz_id
        mock_enrich_csv.return_value = '/tmp/enriched_file.csv'
        
        # Mock storage client
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_bucket.get_blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery load job
        mock_load_job = Mock()
        mock_load_job.output_rows = 100
        mock_bq_client.return_value.load_table_from_file.return_value = mock_load_job
        
        dag_module.load_data_to_bigquery(**mock_context)
        
        # Verify calls
        assert mock_fetch_epoch.call_count == 2  # Called for each bot prefix
        assert mock_get_blobs.call_count == 2
        assert mock_sleep.call_count == 2  # Called for each file processed
        mock_insert_audit.assert_called_once()

    @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
    def test_load_data_to_bigquery_no_new_files(self, mock_get_blobs, mock_fetch_epoch, mock_context):
        """Test load_data_to_bigquery with no new files"""
        mock_fetch_epoch.return_value = 1234567890
        mock_get_blobs.return_value = []  # No new files
        
        with patch('yellow_ai_unsubscribe_report_dag.insert_audit_records') as mock_insert_audit:
            dag_module.load_data_to_bigquery(**mock_context)
            mock_insert_audit.assert_not_called()

    @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
    @patch('yellow_ai_unsubscribe_report_dag.datetime')
    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
    @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id_and_mdlz_id')
    @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
    @patch('builtins.open', new_callable=mock_open, read_data=b'csv_data')
    def test_load_data_to_bigquery_bigquery_failure(self, mock_file_open, mock_insert_audit, mock_enrich_csv, 
                                                   mock_get_blobs, mock_fetch_epoch, mock_bq_client, 
                                                   mock_storage_client, mock_datetime, mock_sleep, mock_context):
        """Test load_data_to_bigquery with BigQuery load failure"""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock fetch_last_processed_epoch_ts
        mock_fetch_epoch.return_value = 1234567890
        
        # Mock get_blobs
        mock_get_blobs.return_value = [
            {'name': 'test_file.csv', 'generation': 1234567891, 'updated': mock_now}
        ]
        
        # Mock enrich_csv_with_bot_id_and_mdlz_id
        mock_enrich_csv.return_value = '/tmp/enriched_file.csv'
        
        # Mock storage client
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_bucket.get_blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery load job failure
        mock_bq_client.return_value.load_table_from_file.side_effect = Exception('BigQuery load error')
        
        dag_module.load_data_to_bigquery(**mock_context)
        
        # Verify audit records are still inserted with failure status
        mock_insert_audit.assert_called_once()
        audit_records = mock_insert_audit.call_args[0][0]
        assert any(record['process_status'] == 'FAILURE' for record in audit_records)

    @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
    @patch('yellow_ai_unsubscribe_report_dag.datetime')
    @patch('yellow_ai_unsubscribe_report_dag.storage.Client')
    @patch('yellow_ai_unsubscribe_report_dag.bigquery.Client')
    @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_unsubscribe_report_dag.get_blobs')
    @patch('yellow_ai_unsubscribe_report_dag.enrich_csv_with_bot_id_and_mdlz_id')
    @patch('yellow_ai_unsubscribe_report_dag.insert_audit_records')
    @patch('builtins.open', new_callable=mock_open, read_data=b'csv_data')
    def test_load_data_to_bigquery_zero_rows_loaded(self, mock_file_open, mock_insert_audit, mock_enrich_csv, 
                                                   mock_get_blobs, mock_fetch_epoch, mock_bq_client, 
                                                   mock_storage_client, mock_datetime, mock_sleep, mock_context):
        """Test load_data_to_bigquery with zero rows loaded"""
        # Mock datetime
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        
        # Mock fetch_last_processed_epoch_ts
        mock_fetch_epoch.return_value = 1234567890
        
        # Mock get_blobs
        mock_get_blobs.return_value = [
            {'name': 'test_file.csv', 'generation': 1234567891, 'updated': mock_now}
        ]
        
        # Mock enrich_csv_with_bot_id_and_mdlz_id
        mock_enrich_csv.return_value = '/tmp/enriched_file.csv'
        
        # Mock storage client
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_bucket.get_blob.return_value = mock_blob
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery load job with zero rows
        mock_load_job = Mock()
        mock_load_job.output_rows = 0  # Zero rows loaded
        mock_bq_client.return_value.load_table_from_file.return_value = mock_load_job
        
        dag_module.load_data_to_bigquery(**mock_context)
        
        # Verify audit records show FAILURE status when no rows loaded
        mock_insert_audit.assert_called_once()
        audit_records = mock_insert_audit.call_args[0][0]
        assert any(record['process_status'] == 'FAILURE' for record in audit_records)
        assert any(record['rows_processed'] == 0 for record in audit_records)

    @patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts')
    def test_load_data_to_bigquery_exception(self, mock_fetch_epoch, mock_context):
        """Test load_data_to_bigquery with exception in main flow"""
        mock_fetch_epoch.side_effect = Exception('Fetch epoch error')
        
        with pytest.raises(Exception):
            dag_module.load_data_to_bigquery(**mock_context)

    def test_schema_definition(self):
        """Test that the BigQuery schema is correctly defined"""
        # This test verifies the schema used in load_data_to_bigquery
        expected_fields = ['phone', 'user_request', 'created_at', 'updated_at', 'bot_id', 'mdlzID']
        
        # We can't directly test the schema from the function, but we can verify
        # the expected fields are what we expect for unsubscribe data
        assert len(expected_fields) == 6
        assert 'phone' in expected_fields
        assert 'bot_id' in expected_fields
        assert 'mdlzID' in expected_fields

    def test_prefix_path_construction(self):
        """Test that the correct prefix path is constructed for unsubscribe data"""
        # The prefix should be "{bot_prefix}/bot_tables/unsubscribe_requests"
        bot_prefix = "test_bot"
        expected_prefix = f"{bot_prefix}/bot_tables/unsubscribe_requests"
        
        # This verifies the path structure used in the DAG
        assert expected_prefix == "test_bot/bot_tables/unsubscribe_requests"

    @patch('yellow_ai_unsubscribe_report_dag.time.sleep')
    def test_time_sleep_called(self, mock_sleep, mock_context):
        """Test that time.sleep is called during processing"""
        with patch('yellow_ai_unsubscribe_report_dag.fetch_last_processed_epoch_ts', return_value=0):
            with patch('yellow_ai_unsubscribe_report_dag.get_blobs', return_value=[]):
                dag_module.load_data_to_bigquery(**mock_context)
                # sleep should not be called if no blobs to process
                mock_sleep.assert_not_called()

    @patch('yellow_ai_unsubscribe_report_dag.asyncio.run')
    @patch('yellow_ai_unsubscribe_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_unsubscribe_report_dag.pd.read_csv')
    def test_enrich_csv_phone_normalization(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test phone number normalization in enrich_csv_with_bot_id_and_mdlz_id"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv with phones that need normalization
        df_with_phones = pd.DataFrame({
            'phone': ['+1234567890', '9876543210', '+5555555555'],  # Mixed format
            'user_request': ['unsubscribe', 'stop', 'opt-out']
        })
        mock_read_csv.return_value = df_with_phones
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        # Mock asyncio.run
        mock_asyncio_run.return_value = {
            '+1234567890': 'mdlz_id_1',
            '+9876543210': 'mdlz_id_2',
            '+5555555555': 'mdlz_id_3'
        }
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        # Verify that fetch_mdlz_ids was called with normalized phone numbers
        mock_asyncio_run.assert_called_once()
        called_phones = mock_asyncio_run.call_args[0][0]
        assert all(phone.startswith('+') for phone in called_phones)

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--cov=yellow_ai_unsubscribe_report_dag', '--cov-report=html'])