"""
PyODIBEL: Open Data Integration Benchmark Evaluation Lab

A library for generating and evaluating benchmark datasets for data integration tasks
across different domains, built from Linked Open Data (LOD) sources and large public dumps.
"""

import logging

# Core interfaces - import from api module
from pyodibel.api import (
    Entity,
    EntityMetadata,
    SimpleEntity,
    Source,
    SourceConfig,
    Operation,
    OperationConfig,
    EntityOperation,
    BatchOperation,
    Benchmark,
    BenchmarkConfig,
    BenchmarkBuilder,
    BenchmarkSplit,
    SplitType,
    Evaluator,
    EvaluationConfig,
    EvaluationResult,
)

# Optional imports for modules that may not exist yet
try:
    from pyodibel.api.storage import StorageBackend, StorageConfig
except ImportError:
    StorageBackend = None
    StorageConfig = None

try:
    from pyodibel.api.partition import PartitionStrategy, PartitionConfig
except ImportError:
    PartitionStrategy = None
    PartitionConfig = None

# Initialize backend registry (registers default backends and format handlers)
try:
    from pyodibel.api.backend import get_backend_registry, backend_registry
except ImportError:
    pass  # Backend registry will be initialized when needed

# Data sources
try:
    from pyodibel.source.wikidata.dump import WikidataDumpSource
except ImportError:
    WikidataDumpSource = None

try:
    from pyodibel.source.linked_data.source import LinkedDataSource
except ImportError:
    LinkedDataSource = None

__version__ = "0.1.0"

__all__ = [
    # Core interfaces
    "Entity",
    "EntityMetadata",
    "SimpleEntity",
    "Source",
    "SourceConfig",
    "Operation",
    "OperationConfig",
    "EntityOperation",
    "BatchOperation",
    "Benchmark",
    "BenchmarkConfig",
    "BenchmarkBuilder",
    "BenchmarkSplit",
    "SplitType",
    "Evaluator",
    "EvaluationConfig",
    "EvaluationResult",
    # Optional interfaces
    "StorageBackend",
    "StorageConfig",
    "PartitionStrategy",
    "PartitionConfig",
    # Data sources
    "WikidataDumpSource",
    "LinkedDataSource",
]


def setup_logging(log_file='pyodibel.log', level=logging.INFO):
    """
    Setup logging configuration for PyODIBEL.
    
    Args:
        log_file: Path to log file
        level: Logging level
    """
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Check if the root logger already has handlers (avoid adding multiple)
    if not root_logger.handlers:
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)

        # Add the handler to the root logger
        root_logger.addHandler(file_handler)

# Call this once at the start of your application
setup_logging()