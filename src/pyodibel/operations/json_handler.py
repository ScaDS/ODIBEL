"""
JSON format handler implementations.

Provides format handlers for JSON/JSONL formats with different backends.
"""

from typing import Any, Optional
from pathlib import Path
import logging

from pyodibel.core.backend import FormatHandler, BackendType

logger = logging.getLogger(__name__)


class SparkJSONFormatHandler(FormatHandler):
    """
    JSON format handler for Spark backend.
    
    Handles JSON and JSONL (JSON Lines) formats with Spark.
    Supports both single JSON files and JSONL files (one JSON object per line).
    """
    
    @property
    def format_name(self) -> str:
        """Get the format name."""
        return "json"
    
    def supports_backend(self, backend_type: BackendType) -> bool:
        """
        Check if this handler supports a specific backend.
        
        Args:
            backend_type: Type of backend
            
        Returns:
            True if Spark backend is supported
        """
        return backend_type == BackendType.SPARK
    
    def read(self, source: Any, backend: Any, **kwargs) -> Any:
        """
        Read JSON/JSONL data using Spark.
        
        Args:
            source: Data source (path to JSON/JSONL file or directory)
            backend: Spark backend instance
            **kwargs: Additional parameters
            
        Returns:
            Spark DataFrame
        """
        spark = backend.get_session()
        source_path = str(source)
        
        # Check if it's a directory or file
        path_obj = Path(source_path)
        
        # For Wikidata dumps, they're often JSONL (one JSON object per line)
        # Spark can read both JSON and JSONL
        # If it's a single large JSON file, Spark will handle it
        # If it's JSONL, Spark will read each line as a separate JSON object
        
        # Allow specifying if it's JSONL format
        is_jsonl = kwargs.pop("jsonl", False)
        
        if is_jsonl:
            # For JSONL, read as text first, then parse JSON
            # Spark can handle this natively with read.json() on JSONL files
            logger.info(f"Reading JSONL file: {source_path}")
            df = spark.read.json(source_path, **kwargs)
        else:
            # For regular JSON (array or object)
            logger.info(f"Reading JSON file: {source_path}")
            df = spark.read.json(source_path, **kwargs)
        
        return df
    
    def write(self, data: Any, target: Any, backend: Any, **kwargs) -> None:
        """
        Write JSON/JSONL data using Spark.
        
        Args:
            data: Spark DataFrame
            target: Target location (path or directory)
            backend: Spark backend instance
            **kwargs: Additional parameters
        """
        mode = kwargs.pop("mode", "overwrite")
        target_path = str(target)
        
        # Write as JSON (Spark will create directory with part files)
        data.write.mode(mode).json(target_path, **kwargs)
        logger.info(f"Wrote JSON data to: {target_path}")


class PandasJSONFormatHandler(FormatHandler):
    """JSON format handler for Pandas backend."""
    
    @property
    def format_name(self) -> str:
        return "json"
    
    def supports_backend(self, backend_type: BackendType) -> bool:
        return backend_type == BackendType.PANDAS
    
    def read(self, source: Any, backend: Any, **kwargs) -> Any:
        import pandas as pd
        return pd.read_json(source, **kwargs)
    
    def write(self, data: Any, target: Any, backend: Any, **kwargs) -> None:
        data.to_json(target, **kwargs)


class NativeJSONFormatHandler(FormatHandler):
    """JSON format handler for Native backend."""
    
    @property
    def format_name(self) -> str:
        return "json"
    
    def supports_backend(self, backend_type: BackendType) -> bool:
        return backend_type == BackendType.NATIVE
    
    def read(self, source: Any, backend: Any, **kwargs) -> Any:
        import json
        with open(source, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def write(self, data: Any, target: Any, backend: Any, **kwargs) -> None:
        import json
        with open(target, 'w', encoding='utf-8') as f:
            json.dump(data, f, **kwargs)



