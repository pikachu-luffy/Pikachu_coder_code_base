import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from helpers.log import get_logger
from helpers.singleton_meta import SingletonMeta

root_folder = Path(__file__).parents[1]


class BaseConfig(metaclass=SingletonMeta):   # need to change accordingly
    def __init__(self):
        self.logger = get_logger()
        config_json = root_folder / "config.json"
        
        try:
            with open(config_json) as file:    
                self.full_config = json.load(file)  
        except Exception as e:            
            self.logger.error(f"Error loading config_json: {e}")

        
        secrets = self.full_config  
        
        main_props = {
            v: list(v.__annotations__.keys())
            for k, v in self.__dict__.items()
            if isinstance(v, DataConfig)
        }

        for object_name, variables_list in main_props.items():
            for variable_name in variables_list:
                value = os.getenv(variable_name.upper())
                if value:
                    setattr(object_name, variable_name, value)
                else:
                    try:
                        value = secrets.get(variable_name)  
                    except:
                        value = None

                    if value:
                        setattr(object_name, variable_name, value)
                    else:
                        self.logger.error(
                            f"Configuration error. {variable_name} is not set."
                        )

    def get_dag_config(self, dag_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific DAG.
        Args:
            dag_name: The DAG identifier
        Returns:
            Dictionary containing DAG-specific configuration or None if not found
        """
        return self.full_config.get('dags', {}).get(dag_name)

    def list_available_dags(self) -> list:
        """Return list of available DAG configurations."""
        return list(self.full_config.get('dags', {}).keys())


class DataConfig:
    pass


class AppConfig(DataConfig):
    project_name: str
    environment: str
    mondelez_id_service_url: str
    batch_size: str
    market: str
    notification_gcs_prefix: str
    # notification_report_prefix: str
    unsubscribe_gcs_prefix: str
    unsubscribe_report_prefix: str


class GCPConfig(DataConfig):
    gcp_global_project_id: str
    gcp_bq_data_project_id: str
    gcp_bq_dataset_id: str
    gcp_bq_notification_table: str
    gcp_bq_unsubscribe_table: str
    gcp_bq_lookup_table: str
    gcp_bq_audit_table: str
    gcp_gcs_data_bucket: str


class Config(BaseConfig):
    app: AppConfig
    gcp: GCPConfig

    def __init__(self):
        self.app = AppConfig()
        self.gcp = GCPConfig()
        super().__init__()
        self.logger.success(f"{self.app.project_name} configured")