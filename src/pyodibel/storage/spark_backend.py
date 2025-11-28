"""
Spark execution backend implementation.

Provides Spark-based execution backend for large-scale distributed processing.
"""

from typing import Any, Optional, Dict
from pathlib import Path
import logging

from pyodibel.core.backend import ExecutionBackend, BackendConfig, BackendType

logger = logging.getLogger(__name__)


class SparkBackend(ExecutionBackend):
    """
    Spark execution backend for distributed data processing.
    
    Supports large-scale data processing using Apache Spark.
    """
    
    def __init__(self, config: BackendConfig):
        """
        Initialize Spark backend.
        
        Args:
            config: Backend configuration with Spark-specific settings
        """
        # Set _spark to None before calling super().__init__() which may call initialize()
        self._spark = None
        super().__init__(config)
    
    def initialize(self) -> None:
        """Initialize Spark session."""
        try:
            from pyspark.sql import SparkSession
            
            # Build Spark session with configuration
            builder = SparkSession.builder
            
            # Set app name
            app_name = self.config.config.get("app_name", "PyODIBEL")
            builder = builder.appName(app_name)
            
            # Set master
            master = self.config.config.get("master", "local[*]")
            builder = builder.master(master)
            
            # Add additional Spark configurations
            spark_config = self.config.config.get("spark_config", {})
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            # Set default configurations if not provided
            if "spark.driver.memory" not in spark_config:
                builder = builder.config("spark.driver.memory", "4g")
            if "spark.executor.memory" not in spark_config:
                builder = builder.config("spark.executor.memory", "4g")
            
            self._spark = builder.getOrCreate()
            self._session = self._spark
            logger.info(f"Spark session initialized: {app_name} on {master}")
            
        except ImportError:
            raise ImportError(
                "PySpark is not installed. Install it with: pip install pyspark"
            )
    
    def shutdown(self) -> None:
        """Shutdown Spark session."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            self._session = None
            logger.info("Spark session stopped")
    
    def load_data(self, source: Any, format: str, **kwargs) -> Any:
        """
        Load data into Spark DataFrame.
        
        Args:
            source: Data source (path, URL, or data object)
            format: Format identifier (json, jsonl, csv, parquet, etc.)
            **kwargs: Additional format-specific parameters
            
        Returns:
            Spark DataFrame
        """
        if self._spark is None:
            self.initialize()
        
        if format == "json" or format == "jsonl":
            # For JSON/JSONL files (including Wikidata dumps)
            # Spark can read both JSON arrays and JSONL natively
            # Remove jsonl parameter if present (Spark doesn't need it)
            spark_kwargs = {k: v for k, v in kwargs.items() if k != "jsonl"}
            return self._spark.read.json(str(source), **spark_kwargs)
        elif format == "csv":
            return self._spark.read.csv(str(source), header=True, inferSchema=True, **kwargs)
        elif format == "parquet":
            return self._spark.read.parquet(str(source), **kwargs)
        elif format == "text":
            return self._spark.read.text(str(source), **kwargs)
        else:
            raise ValueError(f"Unsupported format for Spark: {format}")
    
    def save_data(self, data: Any, target: Any, format: str, **kwargs) -> None:
        """
        Save Spark DataFrame to target location.
        
        Args:
            data: Spark DataFrame
            target: Target location (path, URL, etc.)
            format: Format identifier
            **kwargs: Additional format-specific parameters
        """
        if self._spark is None:
            raise RuntimeError("Spark session not initialized")
        
        # Get write mode
        mode = kwargs.pop("mode", "overwrite")
        
        if format == "json" or format == "jsonl":
            data.write.mode(mode).json(str(target), **kwargs)
        elif format == "csv":
            data.write.mode(mode).csv(str(target), header=True, **kwargs)
        elif format == "parquet":
            data.write.mode(mode).parquet(str(target), **kwargs)
        elif format == "text":
            data.write.mode(mode).text(str(target), **kwargs)
        else:
            raise ValueError(f"Unsupported format for Spark: {format}")
    
    def supports_format(self, format: str) -> bool:
        """
        Check if Spark supports a specific format.
        
        Args:
            format: Format identifier
            
        Returns:
            True if format is supported, False otherwise
        """
        supported_formats = ["json", "jsonl", "csv", "parquet", "text"]
        return format in supported_formats
    
    def get_session(self) -> Any:
        """Get Spark session."""
        if self._spark is None:
            self.initialize()
        return self._spark

