"""
Wikidata dump data source implementation.

Handles ingestion of Wikidata JSON dumps, which are large files containing
JSON arrays or JSONL (one JSON object per line) where each element is a
Wikidata entity.

This source reads the dump as text, parses each line as JSON, and maps
to WikidataEntity Pydantic models.
"""

from typing import Any, Iterator, Optional, Dict, List
from pathlib import Path
import json
import logging

from pyodibel.core.data_source import DataSource, DataSourceConfig, StreamingDataSource
from pyodibel.core.backend import BackendConfig, BackendType, ExecutionBackend, get_backend_registry
from pyodibel.source.wikidata.entity import WikidataEntity

logger = logging.getLogger(__name__)


class WikidataDumpSource(StreamingDataSource):
    """
    Data source for Wikidata JSON dumps.
    
    Wikidata dumps are typically:
    - Large JSON files (can be GBs or TBs)
    - JSONL format (one JSON object per line) or JSON arrays
    - Each line/object represents a Wikidata entity
    
    This source:
    1. Reads the dump file as text (line by line) using Spark
    2. Parses each line as JSON
    3. Maps to WikidataEntity Pydantic models
    4. Filters out invalid JSON lines
    
    The Spark backend is used for the internal entity representation,
    not for the raw Wikidata JSON format.
    """
    
    def __init__(self, config: DataSourceConfig, backend_config: Optional[BackendConfig] = None):
        """
        Initialize Wikidata dump source.
        
        Args:
            config: Data source configuration
            backend_config: Optional backend configuration (defaults to Spark)
        """
        super().__init__(config)
        
        # Default to Spark backend for large file processing
        if backend_config is None:
            backend_config = BackendConfig(
                backend_type=BackendType.SPARK,
                config={
                    "app_name": "WikidataDumpIngestion",
                    "master": "local[*]",
                    "spark_config": {
                        "spark.driver.memory": "8g",
                        "spark.executor.memory": "8g",
                        "spark.sql.shuffle.partitions": "200",
                    }
                }
            )
        
        self.backend_config = backend_config
        self._backend: Optional[ExecutionBackend] = None
    
    def _get_backend(self) -> ExecutionBackend:
        """Get or create the execution backend."""
        if self._backend is None:
            registry = get_backend_registry()
            self._backend = registry.get_backend(
                self.backend_config.backend_type,
                self.backend_config
            )
        return self._backend
    
    def fetch(self, dump_path: str, **kwargs) -> Iterator[WikidataEntity]:
        """
        Fetch entities from Wikidata dump.
        
        Args:
            dump_path: Path to Wikidata dump file (JSON or JSONL)
            **kwargs: Additional parameters:
                - limit: Maximum number of entities to fetch (None for all)
                - filters: Dictionary of filters to apply (on WikidataEntity fields)
                - validate: Whether to validate Pydantic models (default: True)
        
        Yields:
            WikidataEntity instances
        """
        dump_path_obj = Path(dump_path)
        if not dump_path_obj.exists():
            raise FileNotFoundError(f"Wikidata dump not found: {dump_path}")
        
        backend = self._get_backend()
        validate = kwargs.get("validate", True)
        limit = kwargs.get("limit")
        filters = kwargs.get("filters", {})
        
        logger.info(f"Loading Wikidata dump from: {dump_path}")
        
        # Read as text file (line by line)
        text_df = backend.load_data(dump_path, format="text")
        
        # Map each line to entity dict (work with dicts in Spark for serialization)
        # Spark text files return Row objects with 'value' field containing the line
        # Use a pure Python function that doesn't access SparkContext
        # Import the function at module level to avoid capturing self
        parse_func = WikidataDumpSource._parse_json_line_to_dict
        entities_dict_rdd = text_df.rdd.flatMap(
            lambda row: parse_func(row.value)
        )
        
        # Apply filters if provided
        if filters:
            entities_dict_rdd = self._apply_filters_rdd(entities_dict_rdd, filters)
        
        # Apply limit if specified
        if limit:
            entities_dicts = entities_dict_rdd.take(limit)
        else:
            entities_dicts = entities_dict_rdd.collect()
        
        logger.info(f"Fetched {len(entities_dicts)} entities from Wikidata dump")
        
        # Convert dicts to WikidataEntity on driver side (not in Spark transformation)
        for entity_dict in entities_dicts:
            if validate:
                entity = WikidataEntity(**entity_dict)
            else:
                # Create without validation (faster but less safe)
                entity = WikidataEntity.model_construct(**entity_dict)
            yield entity
    
    def fetch_stream(self, dump_path: str, chunk_size: int = 1000, **kwargs) -> Iterator[List[WikidataEntity]]:
        """
        Fetch entities from Wikidata dump in streaming chunks.
        
        Args:
            dump_path: Path to Wikidata dump file
            chunk_size: Number of entities per chunk
            **kwargs: Additional parameters (see fetch())
        
        Yields:
            Lists of WikidataEntity instances (chunks)
        """
        dump_path_obj = Path(dump_path)
        if not dump_path_obj.exists():
            raise FileNotFoundError(f"Wikidata dump not found: {dump_path}")
        
        backend = self._get_backend()
        validate = kwargs.get("validate", True)
        filters = kwargs.get("filters", {})
        
        logger.info(f"Streaming Wikidata dump from: {dump_path}")
        
        # Read as text file
        text_df = backend.load_data(dump_path, format="text")
        
        # Map each line to entity dict (pure Python function, no SparkContext access)
        # Use static method to avoid capturing self
        parse_func = WikidataDumpSource._parse_json_line_to_dict
        entities_dict_rdd = text_df.rdd.flatMap(
            lambda row: parse_func(row.value)
        )
        
        # Apply filters if provided
        if filters:
            entities_dict_rdd = self._apply_filters_rdd(entities_dict_rdd, filters)
        
        # Process in chunks using partitions
        # Collect all partition data, then process in chunks on driver side
        partition_data = entities_dict_rdd.mapPartitions(
            lambda partition: [list(partition)]
        ).collect()
        
        # Flatten partition data and process in chunks
        all_dicts = []
        for partition_list in partition_data:
            all_dicts.extend(partition_list)
        
        # Process in chunks on driver side
        for i in range(0, len(all_dicts), chunk_size):
            chunk_dicts = all_dicts[i:i + chunk_size]
            
            # Convert dicts to WikidataEntity on driver side
            chunk = []
            for d in chunk_dicts:
                if d is not None:
                    if validate:
                        entity = WikidataEntity(**d)
                    else:
                        # Create without validation (faster but less safe)
                        entity = WikidataEntity.model_construct(**d)
                    chunk.append(entity)
            
            if chunk:
                yield chunk
                logger.debug(f"Processed chunk with {len(chunk)} entities")
    
    @staticmethod
    def _parse_json_line_to_dict(line: str) -> List[Dict[str, Any]]:
        """
        Parse a single JSON line to entity dictionary.
        
        This is a pure Python function that runs in Spark transformations.
        It does NOT create Pydantic models (which would require SparkContext).
        Models are created on the driver side after collect().
        
        Args:
            line: Single text line containing JSON
        
        Returns:
            List with one entity dictionary, or empty list if invalid
        """
        line = line.strip()
        
        # Skip empty lines
        if not line:
            return []
        
        # Skip JSON array markers (for array format)
        if line in ["[", "]"]:
            return []
        
        # Remove trailing comma if present (for JSON array format)
        if line.endswith(","):
            line = line.rstrip(",")
        
        try:
            # Parse JSON to dict (no Pydantic model creation here)
            data = json.loads(line)
            
            # Return as list with single dict (for flatMap compatibility)
            return [data]
        
        except json.JSONDecodeError:
            # Invalid JSON - skip silently
            return []
        except Exception:
            # Any other error - skip silently
            return []
    
    def _apply_filters_rdd(self, entities_dict_rdd, filters: Dict[str, Any]):
        """
        Apply filters to RDD of entity dictionaries.
        
        Args:
            entities_dict_rdd: RDD of entity dictionaries
            filters: Dictionary of filter conditions
        
        Returns:
            Filtered RDD of dictionaries
        """
        # Use static method to avoid capturing self
        check_filter_func = WikidataDumpSource._check_filter_dict
        
        def matches_filters(entity_dict: Dict[str, Any]) -> bool:
            """Check if entity matches all filters."""
            for field, value in filters.items():
                if not check_filter_func(entity_dict, field, value):
                    return False
            return True
        
        return entities_dict_rdd.filter(matches_filters)
    
    @staticmethod
    def _check_filter_dict(entity_dict: Dict[str, Any], field: str, value: Any) -> bool:
        """
        Check if entity dict matches a single filter condition.
        
        Args:
            entity_dict: Entity as dictionary
            field: Field name to filter on (supports dot notation like "labels.en.value")
            value: Filter value or operator dict
        
        Returns:
            True if entity matches filter
        """
        # Get field value (support dot notation)
        if "." in field:
            parts = field.split(".")
            field_value = entity_dict
            for part in parts:
                if isinstance(field_value, dict):
                    field_value = field_value.get(part)
                else:
                    return False
                if field_value is None:
                    break
        else:
            field_value = entity_dict.get(field)
        
        if isinstance(value, dict):
            # Support operators
            if "$eq" in value:
                return field_value == value["$eq"]
            elif "$ne" in value:
                return field_value != value["$ne"]
            elif "$in" in value:
                return field_value in value["$in"]
            elif "$exists" in value:
                exists = field_value is not None
                return exists == value["$exists"]
            elif "$has" in value:
                # Check if value contains substring
                return value["$has"] in str(field_value) if field_value else False
        else:
            # Simple equality
            return field_value == value
        
        return False
    
    def supports_format(self, format: str) -> bool:
        """
        Check if the source supports a specific format.
        
        Args:
            format: Format identifier
            
        Returns:
            True if JSON or JSONL format is supported
        """
        return format in ["json", "jsonl"]
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the data source."""
        metadata = super().get_metadata()
        metadata.update({
            "backend_type": self.backend_config.backend_type.value,
            "supports_streaming": True,
            "entity_model": "WikidataEntity",
        })
        return metadata
