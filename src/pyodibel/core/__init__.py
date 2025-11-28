"""
Core abstractions and interfaces for PyODIBEL.

This module contains abstract base classes and protocols that define the
interfaces for all major components in the PyODIBEL library.
"""

from pyodibel.core.data_source import DataSource, DataSourceConfig
from pyodibel.core.storage import StorageBackend, StorageConfig
from pyodibel.core.operations import DataOperation, OperationConfig
from pyodibel.core.partition import PartitionStrategy, PartitionConfig
from pyodibel.core.benchmark import BenchmarkBuilder, BenchmarkConfig
from pyodibel.core.backend import (
    ExecutionBackend,
    BackendType,
    BackendConfig,
    FormatHandler,
    BackendRegistry,
    get_backend_registry,
)

__all__ = [
    "DataSource",
    "DataSourceConfig",
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

