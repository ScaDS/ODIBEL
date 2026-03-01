"""
Core abstractions and interfaces for PyODIBEL.

This module contains abstract base classes and protocols that define the
interfaces for all major components in the PyODIBEL library.
"""

from pyodibel.api.data_source import DataSource, DataSourceConfig
from pyodibel.api.entity import DataEntity
from pyodibel.api.operations import DataOperation, OperationConfig
from pyodibel.api.benchmark import BenchmarkBuilder, BenchmarkConfig

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

try:
    from pyodibel.api.backend import (
        ExecutionBackend,
        BackendType,
        BackendConfig,
        FormatHandler,
        BackendRegistry,
        get_backend_registry,
    )
except ImportError:
    ExecutionBackend = None
    BackendType = None
    BackendConfig = None
    FormatHandler = None
    BackendRegistry = None
    get_backend_registry = None

__all__ = [
    "DataSource",
    "DataSourceConfig",
    "DataEntity",
    "StorageBackend",
    "StorageConfig",
    "DataOperation",
    "OperationConfig",
    "PartitionStrategy",
    "PartitionConfig",
    "BenchmarkBuilder",
    "BenchmarkConfig",
    "ExecutionBackend",
    "BackendType",
    "BackendConfig",
    "FormatHandler",
    "BackendRegistry",
    "get_backend_registry",
]

