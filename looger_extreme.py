import logging
import sys
from datetime import datetime
from typing import Optional
import json

class ETLLogger:
    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_pipeline_start(self, pipeline_name: str, run_id: str):
        self.logger.info(f"Pipeline {pipeline_name} started with run_id: {run_id}")
    
    def log_data_quality_check(self, table_name: str, check_results: dict):
        self.logger.info(f"Data quality check for {table_name}: {json.dumps(check_results)}")
    
    def log_error(self, error: Exception, context: dict = None):
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {},
            "timestamp": datetime.now().isoformat()
        }
        self.logger.error(f"Pipeline error: {json.dumps(error_info)}")

# src/utils/error_handler.py
from functools import wraps
from typing import Callable, Any
import traceback

class ETLErrorHandler:
    def __init__(self, logger: ETLLogger):
        self.logger = logger
    
    def handle_errors(self, retry_count: int = 3):
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                last_exception = None
                
                for attempt in range(retry_count):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        self.logger.log_error(
                            e, 
                            {
                                "function": func.__name__,
                                "attempt": attempt + 1,
                                "args": str(args),
                                "kwargs": str(kwargs)
                            }
                        )
                        
                        if attempt == retry_count - 1:
                            raise last_exception
                        
                        time.sleep(2 ** attempt)  # Exponential backoff
                
                raise last_exception
            return wrapper
        return decorator
