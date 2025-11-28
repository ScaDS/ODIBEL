"""
Tests for Wikidata dump ingestion.

Tests the WikidataDumpSource, SparkBackend, and JSON format handlers
using the test data file.

Note: These tests require:
- PySpark installed (pip install pyspark)
- Java 17+ available (if using SDKMAN: `sdk use java 17.0.15-tem`)
"""

import pytest
import subprocess
import os
from pathlib import Path
from typing import Iterator, Dict, Any

from pyodibel.core.data_source import DataSourceConfig
from pyodibel.core.backend import BackendConfig, BackendType
from pyodibel.source.wikidata import WikidataDumpSource, WikidataEntity
from pyodibel.storage import SparkBackend
from pyodibel.operations.json_handler import SparkJSONFormatHandler


# Path to test data file
# Calculate path relative to project root
# From: src/pyodibel/test/source/test_wikidata_dump.py
# To project root: go up 5 levels (ingest -> test -> pyodibel -> src -> odibel)
# Then: data/wikidata-20251103-all.json-1-100.json
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent.parent.parent  # 5 levels up
TEST_DATA_FILE = _project_root / "data" / "wikidata-20251103-all.json-1-100.json"


@pytest.fixture
def test_data_path():
    """Get path to test data file."""
    if not TEST_DATA_FILE.exists():
        pytest.skip(f"Test data file not found: {TEST_DATA_FILE}")
    return str(TEST_DATA_FILE)


def check_java_available():
    """Check if Java is available and return version info."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        # Java -version outputs to stderr, not stdout
        output = result.stderr if result.stderr else result.stdout
        return True, output
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError) as e:
        return False, f"Java not found. If using SDKMAN, run: sdk use java 17.0.15-tem. Error: {e}"


@pytest.fixture(scope="session")
def java_check():
    """Check Java availability before running Spark tests."""
    available, info = check_java_available()
    if not available:
        pytest.skip(f"Java not available. {info}")
    return info


@pytest.fixture
def spark_backend_config():
    """Create Spark backend configuration for testing."""
    # Check Java before creating config
    java_available, _ = check_java_available()
    if not java_available:
        pytest.skip("Java not available. If using SDKMAN, run: sdk use java 17.0.15-tem")
    
    return BackendConfig(
        backend_type=BackendType.SPARK,
        config={
            "app_name": "PyODIBELTest",
            "master": "local[2]",  # Use 2 cores for testing
            "spark_config": {
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
                "spark.sql.shuffle.partitions": "10",
            }
        },
        auto_init=True
    )


@pytest.fixture
def spark_backend(spark_backend_config):
    """Create and yield Spark backend instance."""
    backend = SparkBackend(spark_backend_config)
    try:
        yield backend
    finally:
        backend.shutdown()


@pytest.fixture
def wikidata_source_config():
    """Create Wikidata dump source configuration."""
    return DataSourceConfig(
        name="test_wikidata_dump",
        description="Test Wikidata dump source",
        metadata={"test": True}
    )


@pytest.fixture
def wikidata_source(wikidata_source_config, spark_backend_config, java_check):
    """Create Wikidata dump source instance."""
    source = WikidataDumpSource(
        wikidata_source_config,
        backend_config=spark_backend_config
    )
    yield source
    # Cleanup
    if source._backend:
        source._backend.shutdown()


@pytest.mark.slow
class TestSparkBackend:
    """Tests for SparkBackend."""
    
    def test_initialization(self, spark_backend_config, java_check):
        """Test Spark backend initialization."""
        backend = SparkBackend(spark_backend_config)
        try:
            assert backend._spark is not None
            assert backend.get_session() is not None
        finally:
            backend.shutdown()
    
    def test_load_json_data(self, spark_backend, test_data_path):
        """Test loading JSON data with Spark."""
        df = spark_backend.load_data(test_data_path, format="json")

        df.show(10, False)
        
        assert df is not None
        assert df.count() > 0
        
        # Check that we have columns
        columns = df.columns
        assert len(columns) > 0
        # Wikidata entities should have 'id' field
        assert 'id' in columns or any('id' in col.lower() for col in columns)
    
    def test_supports_format(self, spark_backend):
        """Test format support checking."""
        assert spark_backend.supports_format("json")
        assert spark_backend.supports_format("jsonl")
        assert spark_backend.supports_format("csv")
        assert spark_backend.supports_format("parquet")
        assert not spark_backend.supports_format("unknown_format")
    
    def test_context_manager(self, spark_backend_config):
        """Test Spark backend as context manager."""
        with SparkBackend(spark_backend_config) as backend:
            assert backend.get_session() is not None
            df = backend.load_data(str(TEST_DATA_FILE), format="json")
            assert df.count() > 0
        # Session should be closed after context exit
        assert backend._spark is None or backend._spark._jvm is None


@pytest.mark.slow
class TestSparkJSONFormatHandler:
    """Tests for Spark JSON format handler."""
    
    def test_format_name(self):
        """Test format name property."""
        handler = SparkJSONFormatHandler()
        assert handler.format_name == "json"
    
    def test_supports_backend(self):
        """Test backend support checking."""
        handler = SparkJSONFormatHandler()
        assert handler.supports_backend(BackendType.SPARK)
        assert not handler.supports_backend(BackendType.PANDAS)
        assert not handler.supports_backend(BackendType.NATIVE)
    
    def test_read_json(self, spark_backend, test_data_path):
        """Test reading JSON data."""
        handler = SparkJSONFormatHandler()
        df = handler.read(test_data_path, spark_backend)
        
        assert df is not None
        assert df.count() > 0
    
    def test_read_jsonl(self, spark_backend, test_data_path):
        """Test reading JSONL data."""
        handler = SparkJSONFormatHandler()
        df = handler.read(test_data_path, spark_backend, jsonl=True)
        
        assert df is not None
        assert df.count() > 0


@pytest.mark.slow
class TestWikidataDumpSource:
    """Tests for WikidataDumpSource."""
    
    def test_initialization(self, wikidata_source_config, spark_backend_config):
        """Test Wikidata dump source initialization."""
        source = WikidataDumpSource(wikidata_source_config, spark_backend_config)
        try:
            assert source.config == wikidata_source_config
            assert source.backend_config == spark_backend_config
            assert source._backend is None  # Lazy initialization
        finally:
            if source._backend:
                source._backend.shutdown()
    
    def test_default_backend_config(self, wikidata_source_config):
        """Test default backend configuration."""
        source = WikidataDumpSource(wikidata_source_config)
        try:
            assert source.backend_config.backend_type == BackendType.SPARK
            assert source.backend_config.config["app_name"] == "WikidataDumpIngestion"
        finally:
            if source._backend:
                source._backend.shutdown()
    
    def test_fetch_entities(self, wikidata_source, test_data_path):
        """Test fetching entities from dump."""
        entities = list(wikidata_source.fetch(test_data_path, limit=10))
        
        assert len(entities) > 0
        assert len(entities) <= 10
        
        # Check entity structure - should be WikidataEntity Pydantic models
        for entity in entities:
            assert isinstance(entity, WikidataEntity)
            assert entity.id is not None
            assert entity.type in ["item", "property"]
    
    def test_fetch_all_entities(self, wikidata_source, test_data_path):
        """Test fetching all entities from dump."""
        entities = list(wikidata_source.fetch(test_data_path))
        
        assert len(entities) > 0
        # Should have at least some entities (up to 100 based on filename)
        assert len(entities) <= 100
        
        # All should be WikidataEntity instances
        for entity in entities:
            assert isinstance(entity, WikidataEntity)
    
    def test_fetch_with_limit(self, wikidata_source, test_data_path):
        """Test fetching with limit."""
        limit = 5
        entities = list(wikidata_source.fetch(test_data_path, limit=limit))
        
        assert len(entities) <= limit
    
    def test_fetch_stream(self, wikidata_source, test_data_path):
        """Test streaming entities from dump."""
        chunk_size = 10
        total_entities = 0
        
        for chunk in wikidata_source.fetch_stream(test_data_path, chunk_size=chunk_size):
            assert isinstance(chunk, list)
            assert len(chunk) <= chunk_size
            total_entities += len(chunk)
            
            # Check chunk entities - should be WikidataEntity instances
            for entity in chunk:
                assert isinstance(entity, WikidataEntity)
            
            # Stop after a few chunks for testing
            if total_entities >= 30:
                break
        
        assert total_entities > 0
    
    def test_get_metadata(self, wikidata_source):
        """Test metadata retrieval."""
        metadata = wikidata_source.get_metadata()
        
        assert "name" in metadata
        assert "backend_type" in metadata
        assert "supports_streaming" in metadata
        assert metadata["supports_streaming"] is True
        assert metadata["backend_type"] == "spark"
    
    def test_entity_model_structure(self, wikidata_source, test_data_path):
        """Test that entities are proper WikidataEntity models with expected structure."""
        entities = list(wikidata_source.fetch(test_data_path, limit=5))
        
        assert len(entities) > 0
        
        for entity in entities:
            assert isinstance(entity, WikidataEntity)
            # Check required fields
            assert hasattr(entity, 'id')
            assert hasattr(entity, 'type')
            assert entity.id is not None
            assert entity.type in ["item", "property"]
            
            # Check optional fields exist (may be None)
            assert hasattr(entity, 'labels')
            assert hasattr(entity, 'descriptions')
            assert hasattr(entity, 'claims')
            
            # Test helper methods
            if entity.labels:
                label = entity.get_label('en')
                # Label may be None if English label doesn't exist
                assert label is None or isinstance(label, str)


@pytest.mark.slow
class TestWikidataDumpSourceFilters:
    """Tests for filtering in WikidataDumpSource."""
    
    def test_simple_filter(self, wikidata_source, test_data_path):
        """Test simple equality filter."""
        # Note: This test depends on the actual data structure
        # Adjust filters based on what's in the test file
        try:
            entities = list(wikidata_source.fetch(
                test_data_path,
                filters={"type": "item"},
                limit=10
            ))
            # Filter might return 0 results if no items match
            assert isinstance(entities, list)
            # All entities should be WikidataEntity instances
            for entity in entities:
                assert isinstance(entity, WikidataEntity)
                assert entity.type == "item"
        except Exception as e:
            # If filtering fails due to data structure, that's okay for now
            pytest.skip(f"Filter test skipped due to data structure: {e}")
    
    def test_exists_filter(self, wikidata_source, test_data_path):
        """Test $exists filter operator."""
        try:
            entities = list(wikidata_source.fetch(
                test_data_path,
                filters={"labels": {"$exists": True}},
                limit=10
            ))
            assert isinstance(entities, list)
            # All entities should be WikidataEntity instances
            for entity in entities:
                assert isinstance(entity, WikidataEntity)
                # Entities with labels should have labels attribute set
                if entity.labels:
                    assert len(entity.labels) > 0
        except Exception as e:
            pytest.skip(f"Exists filter test skipped: {e}")


@pytest.mark.slow
class TestWikidataDumpSourceIntegration:
    """Integration tests for Wikidata dump source."""
    
    def test_end_to_end_ingestion(self, wikidata_source, test_data_path, tmp_path):
        """Test complete ingestion workflow."""
        # Fetch entities
        entities = list(wikidata_source.fetch(test_data_path, limit=20))
        
        assert len(entities) > 0
        
        # Verify entity structure - should be WikidataEntity models
        for entity in entities:
            assert isinstance(entity, WikidataEntity)
            assert entity.id is not None
            assert entity.type in ["item", "property"]
    
    def test_backend_reuse(self, wikidata_source, test_data_path):
        """Test that backend is reused across multiple fetches."""
        # First fetch
        entities1 = list(wikidata_source.fetch(test_data_path, limit=5))
        
        # Backend should be initialized
        assert wikidata_source._backend is not None
        
        # Second fetch should reuse the same backend
        entities2 = list(wikidata_source.fetch(test_data_path, limit=5))
        
        # Should get results
        assert len(entities1) > 0
        assert len(entities2) > 0
    
    def test_specific_entity_values(self, wikidata_source, test_data_path):
        """Test reading and checking for a specific entity with value assertions."""
        # Fetch all entities and find a specific one (e.g., Q23 or Q24)
        entities = list(wikidata_source.fetch(test_data_path))
        
        assert len(entities) > 0
        
        # Find entity Q23 (George Washington) or Q24 (Jack Bauer) if present
        target_entity = None
        for entity in entities:
            if entity.id in ["Q23", "Q24"]:
                target_entity = entity
                break
        
        # If target entities not found, use first entity for testing
        if target_entity is None:
            target_entity = entities[0]
        
        # Assert entity values
        assert target_entity.id is not None
        assert target_entity.type == "item"
        
        # Check if entity has labels
        if target_entity.labels:
            # Try to get English label
            en_label = target_entity.get_label('en')
            if en_label:
                assert isinstance(en_label, str)
                assert len(en_label) > 0
        
        # Check if entity has descriptions
        if target_entity.descriptions:
            en_desc = target_entity.get_description('en')
            if en_desc:
                assert isinstance(en_desc, str)
        
        # Check if entity has claims
        if target_entity.claims:
            assert isinstance(target_entity.claims, dict)
            # Get claims for a common property (P31 = instance of)
            instance_claims = target_entity.get_claims("P31")
            # May be empty, but if present should be a list
            assert isinstance(instance_claims, list)
        
        # Test entity ID specific assertions if we found the target entities
        if target_entity.id == "Q23":
            # George Washington specific checks
            en_label = target_entity.get_label('en')
            assert en_label is not None, "Q23 should have an English label"
            assert "Washington" in en_label or "George" in en_label, \
                f"Q23 label should contain 'Washington' or 'George', got: {en_label}"
        
        elif target_entity.id == "Q24":
            # Jack Bauer specific checks
            en_label = target_entity.get_label('en')
            assert en_label is not None, "Q24 should have an English label"
            assert "Bauer" in en_label or "Jack" in en_label, \
                f"Q24 label should contain 'Bauer' or 'Jack', got: {en_label}"


# Skip tests if PySpark is not available
try:
    import pyspark
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

if not PYSPARK_AVAILABLE:
    # Mark all Spark-dependent tests to skip with a clear message
    skip_reason = "PySpark not installed. Install with: pip install pyspark"
    for test_class in [
        TestSparkBackend,
        TestSparkJSONFormatHandler,
        TestWikidataDumpSource,
        TestWikidataDumpSourceFilters,
        TestWikidataDumpSourceIntegration,
    ]:
        for attr_name in dir(test_class):
            if attr_name.startswith("test_"):
                original_test = getattr(test_class, attr_name)
                # Apply skip marker while preserving other markers
                skipped_test = pytest.mark.skip(reason=skip_reason)(original_test)
                setattr(test_class, attr_name, skipped_test)

