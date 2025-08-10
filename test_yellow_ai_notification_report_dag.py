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

os.environ['AIRFLOW_VAR_IN_YA_NOTIFICATION_REP_CONFIG'] = json.dumps(test_config)

TEST_FILE_PATH = Path(__file__).resolve()
PROJECT_ROOT = TEST_FILE_PATH.parents[2]
DAGS_DIR = PROJECT_ROOT / "source" / "composer" / "dags"

sys.path.insert(0, str(DAGS_DIR))

dag_logic_file = DAGS_DIR / "yellow_ai_notification_report_dag.py"
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
        import yellow_ai_notification_report_dag as dag_module
        print("Successfully imported yellow_ai_notification_report_dag")
    except ImportError as e:
        print(f"Failed to import yellow_ai_notification_report_dag: {e}")
        # Try with importlib if direct import fails
        try:
            import importlib.util
            dag_file = DAGS_DIR / "yellow_ai_notification_report_dag.py"
            spec = importlib.util.spec_from_file_location("yellow_ai_notification_report_dag", dag_file)
            if spec and spec.loader:
                dag_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(dag_module)
                print("Successfully imported using importlib")
            else:
                raise ImportError("Could not create module spec")
        except Exception as e2:
            print(f"Failed to import using importlib: {e2}")
            raise ImportError(f"Could not import DAG module: {e}, {e2}")


class TestYellowAINotificationReportDAG:
    """Test suite for Yellow AI Notification Report DAG"""

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
    def sample_csv_data(self):
        """Sample CSV data for testing"""
        return pd.DataFrame({
            '_id': ['id1', 'id2', 'id3'],
            'campaignId': ['camp1', 'camp2', 'camp3'],
            'botId': ['bot1', 'bot2', 'bot3'],
            'userId': ['1234567890', '9876543210', '5555555555'],
            'status': ['sent', 'delivered', 'read']
        })

    def test_dag_structure(self):
        """Test DAG structure and configuration"""
        assert hasattr(dag_module, 'dag')
        assert dag_module.dag.dag_id == 'in_ya_notification_report_load_dag'
        assert dag_module.dag.schedule_interval == '30 11 * * *'
        assert dag_module.dag.catchup is False
        
        # Test tasks exist
        task_ids = [task.task_id for task in dag_module.dag.tasks]
        expected_tasks = ['start', 'run_notfication_report_load', 'end']
        assert all(task_id in task_ids for task_id in expected_tasks)

    def test_pipeline_config(self):
        """Test pipeline configuration"""
        assert dag_module.PIPELINE_CONFIG['project_id'] == 'test_project_id'
        assert dag_module.PIPELINE_CONFIG['dataset'] == 'test_dataset'
        assert dag_module.PIPELINE_CONFIG['table_id'] == 'test_table_id'
        assert dag_module.PIPELINE_CONFIG['bucket_name'] == 'test_bucket_name'
        assert dag_module.PIPELINE_CONFIG['audit_table_id'] == 'test_audit_table_id'
        assert dag_module.PIPELINE_CONFIG['bot_id_prefix'] == ['test_bot_id_prefix_1', 'test_bot_id_prefix_2']

    def test_log_task_failure_success(self, mock_context):
        """Test log_task_failure function with valid context"""
        with patch('yellow_ai_notification_report_dag.logging') as mock_logging:
            dag_module.log_task_failure(mock_context)
            mock_logging.error.assert_called_once()
            
    def test_log_task_failure_exception(self):
        """Test log_task_failure function with invalid context"""
        invalid_context = {}
        with patch('yellow_ai_notification_report_dag.logging') as mock_logging:
            with pytest.raises(Exception):
                dag_module.log_task_failure(invalid_context)
            mock_logging.error.assert_called()

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_success(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with successful query"""
        # Mock BigQuery client and query result
        mock_query_result = Mock()
        mock_query_result.max_gen = 1234567890
        mock_bq_client.return_value.query.return_value.result.return_value = iter([mock_query_result])
        
        result = dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)
        
        assert result == 1234567890
        mock_bq_client.assert_called_once_with(project='test_project_id')

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_no_result(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with no previous results"""
        # Mock BigQuery client with None result
        mock_query_result = Mock()
        mock_query_result.max_gen = None
        mock_bq_client.return_value.query.return_value.result.return_value = iter([mock_query_result])
        
        result = dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)
        
        assert result == 0

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_fetch_last_processed_epoch_ts_exception(self, mock_bq_client, mock_context):
        """Test fetch_last_processed_epoch_ts with exception"""
        mock_bq_client.return_value.query.side_effect = Exception('BigQuery error')
        
        with pytest.raises(Exception):
            dag_module.fetch_last_processed_epoch_ts('test_bot', mock_context)

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_insert_audit_records_success(self, mock_bq_client, mock_context):
        """Test insert_audit_records with successful insertion"""
        mock_bq_client.return_value.insert_rows_json.return_value = []
        
        test_records = [{'test': 'record1'}, {'test': 'record2'}]
        dag_module.insert_audit_records(test_records, mock_context)
        
        mock_bq_client.return_value.insert_rows_json.assert_called_once_with(
            'test_audit_table_id', test_records
        )

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_insert_audit_records_with_errors(self, mock_bq_client, mock_context):
        """Test insert_audit_records with insertion errors"""
        mock_bq_client.return_value.insert_rows_json.return_value = ['Error 1', 'Error 2']
        
        test_records = [{'test': 'record1'}]
        with pytest.raises(RuntimeError):
            dag_module.insert_audit_records(test_records, mock_context)

    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    def test_insert_audit_records_exception(self, mock_bq_client, mock_context):
        """Test insert_audit_records with exception"""
        mock_bq_client.side_effect = Exception('BigQuery connection error')
        
        test_records = [{'test': 'record1'}]
        with pytest.raises(Exception):
            dag_module.insert_audit_records(test_records, mock_context)

    @patch('yellow_ai_notification_report_dag.storage.Client')
    def test_get_blobs_success(self, mock_storage_client, mock_context):
        """Test get_blobs with successful blob listing"""
        # Mock blob objects
        mock_blob1 = Mock()
        mock_blob1.name = 'test_file1.csv'
        mock_blob1.generation = 1234567891
        mock_blob1.updated = datetime(2023, 1, 1)
        
        mock_blob2 = Mock()
        mock_blob2.name = 'test_file2.json'
        mock_blob2.generation = 1234567892
        mock_blob2.updated = datetime(2023, 1, 2)
        
        mock_blob3 = Mock()
        mock_blob3.name = 'test_file3.txt'  # Should be filtered out
        mock_blob3.generation = 1234567893
        mock_blob3.updated = datetime(2023, 1, 3)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 2
        assert result[0]['name'] == 'test_file1.csv'
        assert result[1]['name'] == 'test_file2.json'

    @patch('yellow_ai_notification_report_dag.storage.Client')
    def test_get_blobs_no_new_files(self, mock_storage_client, mock_context):
        """Test get_blobs with no new files"""
        mock_blob = Mock()
        mock_blob.name = 'old_file.csv'
        mock_blob.generation = 1234567889  # Older than landing_time
        mock_blob.updated = datetime(2023, 1, 1)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 0

    @patch('yellow_ai_notification_report_dag.storage.Client')
    def test_get_blobs_exception(self, mock_storage_client, mock_context):
        """Test get_blobs with exception"""
        mock_storage_client.return_value.list_blobs.side_effect = Exception('Storage error')
        
        with pytest.raises(Exception):
            dag_module.get_blobs(1234567890, 'test_prefix', mock_context)

    @patch('yellow_ai_notification_report_dag.asyncio.run')
    @patch('yellow_ai_notification_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_notification_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_success(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context, sample_csv_data):
        """Test enrich_csv_with_bot_id_and_mdlz_id with successful processing"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv
        mock_read_csv.return_value = sample_csv_data.copy()
        
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

    @patch('yellow_ai_notification_report_dag.asyncio.run')
    @patch('yellow_ai_notification_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_notification_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_no_userid(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id without userId column"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv - DataFrame without userId
        df_without_userid = pd.DataFrame({
            '_id': ['id1', 'id2'],
            'campaignId': ['camp1', 'camp2'],
            'status': ['sent', 'delivered']
        })
        mock_read_csv.return_value = df_without_userid
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        mock_asyncio_run.assert_not_called()

    @patch('yellow_ai_notification_report_dag.pd.read_csv')
    def test_enrich_csv_with_bot_id_and_mdlz_id_exception(self, mock_read_csv, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id with exception"""
        mock_blob = Mock()
        mock_blob.download_as_bytes.side_effect = Exception('Download error')
        
        with pytest.raises(Exception):
            dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)

    @patch('yellow_ai_notification_report_dag.datetime')
    @patch('yellow_ai_notification_report_dag.storage.Client')
    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    @patch('yellow_ai_notification_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_notification_report_dag.get_blobs')
    @patch('yellow_ai_notification_report_dag.enrich_csv_with_bot_id_and_mdlz_id')
    @patch('yellow_ai_notification_report_dag.insert_audit_records')
    def test_load_data_to_bigquery_success(self, mock_insert_audit, mock_enrich_csv, mock_get_blobs, 
                                         mock_fetch_epoch, mock_bq_client, mock_storage_client, 
                                         mock_datetime, mock_context):
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
        mock_processed_blob = Mock()
        mock_bucket.get_blob.return_value = mock_blob
        mock_bucket.blob.return_value = mock_processed_blob
        mock_bucket.list_blobs.return_value = [mock_processed_blob]
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery load job
        mock_load_job = Mock()
        mock_load_job.output_rows = 100
        mock_bq_client.return_value.load_table_from_uri.return_value = mock_load_job
        
        dag_module.load_data_to_bigquery(**mock_context)
        
        # Verify calls
        assert mock_fetch_epoch.call_count == 2  # Called for each bot prefix
        assert mock_get_blobs.call_count == 2
        mock_insert_audit.assert_called_once()

    @patch('yellow_ai_notification_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_notification_report_dag.get_blobs')
    def test_load_data_to_bigquery_no_new_files(self, mock_get_blobs, mock_fetch_epoch, mock_context):
        """Test load_data_to_bigquery with no new files"""
        mock_fetch_epoch.return_value = 1234567890
        mock_get_blobs.return_value = []  # No new files
        
        with patch('yellow_ai_notification_report_dag.insert_audit_records') as mock_insert_audit:
            dag_module.load_data_to_bigquery(**mock_context)
            mock_insert_audit.assert_not_called()

    @patch('yellow_ai_notification_report_dag.datetime')
    @patch('yellow_ai_notification_report_dag.storage.Client')
    @patch('yellow_ai_notification_report_dag.bigquery.Client')
    @patch('yellow_ai_notification_report_dag.fetch_last_processed_epoch_ts')
    @patch('yellow_ai_notification_report_dag.get_blobs')
    @patch('yellow_ai_notification_report_dag.enrich_csv_with_bot_id_and_mdlz_id')
    @patch('yellow_ai_notification_report_dag.insert_audit_records')
    def test_load_data_to_bigquery_bigquery_failure(self, mock_insert_audit, mock_enrich_csv, 
                                                   mock_get_blobs, mock_fetch_epoch, mock_bq_client, 
                                                   mock_storage_client, mock_datetime, mock_context):
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
        mock_processed_blob = Mock()
        mock_bucket.get_blob.return_value = mock_blob
        mock_bucket.blob.return_value = mock_processed_blob
        mock_bucket.list_blobs.return_value = [mock_processed_blob]
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        
        # Mock BigQuery load job failure
        mock_bq_client.return_value.load_table_from_uri.side_effect = Exception('BigQuery load error')
        
        dag_module.load_data_to_bigquery(**mock_context)
        
        # Verify audit records are still inserted with failure status
        mock_insert_audit.assert_called_once()
        audit_records = mock_insert_audit.call_args[0][0]
        assert any(record['process_status'] == 'FAILURE' for record in audit_records)

    @patch('yellow_ai_notification_report_dag.fetch_last_processed_epoch_ts')
    def test_load_data_to_bigquery_exception(self, mock_fetch_epoch, mock_context):
        """Test load_data_to_bigquery with exception in main flow"""
        mock_fetch_epoch.side_effect = Exception('Fetch epoch error')
        
        with pytest.raises(Exception):
            dag_module.load_data_to_bigquery(**mock_context)

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
        load_task = dag_module.dag.get_task('run_notfication_report_load')
        end_task = dag_module.dag.get_task('end')
        
        # Check upstream dependencies
        assert start_task.task_id in [t.task_id for t in load_task.upstream_list]
        assert load_task.task_id in [t.task_id for t in end_task.upstream_list]
        
        # Check downstream dependencies
        assert load_task.task_id in [t.task_id for t in start_task.downstream_list]
        assert end_task.task_id in [t.task_id for t in load_task.downstream_list]

    @patch('yellow_ai_notification_report_dag.storage.Client')
    def test_get_blobs_mixed_file_types(self, mock_storage_client, mock_context):
        """Test get_blobs with mixed file types (CSV, JSON, and others)"""
        # Mock blob objects with different file types
        mock_blob_csv = Mock()
        mock_blob_csv.name = 'test_file.CSV'  # Test case insensitive
        mock_blob_csv.generation = 1234567891
        mock_blob_csv.updated = datetime(2023, 1, 1)
        
        mock_blob_json = Mock()
        mock_blob_json.name = 'test_file.JSON'  # Test case insensitive
        mock_blob_json.generation = 1234567892
        mock_blob_json.updated = datetime(2023, 1, 2)
        
        mock_blob_other = Mock()
        mock_blob_other.name = 'test_file.xml'  # Should be filtered out
        mock_blob_other.generation = 1234567893
        mock_blob_other.updated = datetime(2023, 1, 3)
        
        mock_storage_client.return_value.list_blobs.return_value = [mock_blob_csv, mock_blob_json, mock_blob_other]
        
        result = dag_module.get_blobs(1234567890, 'test_prefix', mock_context)
        
        assert len(result) == 2
        assert any(blob['name'] == 'test_file.CSV' for blob in result)
        assert any(blob['name'] == 'test_file.JSON' for blob in result)

    @patch('yellow_ai_notification_report_dag.asyncio.run')
    @patch('yellow_ai_notification_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_notification_report_dag.pd.read_csv')
    def test_enrich_csv_id_column_rename(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id with _id column rename"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv with _id column
        df_with_underscore_id = pd.DataFrame({
            '_id': ['id1', 'id2'],
            'campaignId': ['camp1', 'camp2'],
            'userId': ['1234567890', '9876543210']
        })
        mock_read_csv.return_value = df_with_underscore_id
        
        # Mock tempfile
        mock_temp = Mock()
        mock_temp.name = '/tmp/test_file.csv'
        mock_tempfile.return_value = mock_temp
        
        # Mock asyncio.run
        mock_asyncio_run.return_value = {'+1234567890': 'mdlz_id_1', '+9876543210': 'mdlz_id_2'}
        
        result = dag_module.enrich_csv_with_bot_id_and_mdlz_id(mock_blob, 'test_bot', mock_context)
        
        assert result == '/tmp/test_file.csv'
        # Verify that _id was renamed to Id
        mock_read_csv.return_value.rename.assert_called_with(columns={'_id': 'Id'}, inplace=True)

    @patch('yellow_ai_notification_report_dag.asyncio.run')
    @patch('yellow_ai_notification_report_dag.tempfile.NamedTemporaryFile')
    @patch('yellow_ai_notification_report_dag.pd.read_csv')
    def test_enrich_csv_empty_phones(self, mock_read_csv, mock_tempfile, mock_asyncio_run, mock_context):
        """Test enrich_csv_with_bot_id_and_mdlz_id with empty phone list"""
        # Mock blob
        mock_blob = Mock()
        mock_blob.download_as_bytes.return_value = b'csv_content'
        mock_blob.name = 'test_file.csv'
        
        # Mock pandas read_csv with empty/null userIds
        df_empty_phones = pd.DataFrame({
            'Id': ['id1', 'id2'],
            'campaignId': ['camp1', 'camp2'],
            'userId': [None, '']  # Empty/null userIds
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

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--cov=yellow_ai_notification_report_dag', '--cov-report=html'])