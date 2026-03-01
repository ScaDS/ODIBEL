"""
API module for PyODIBEL.

Core interfaces for entities, sources, operations, benchmarks, and evaluation.
"""

from pyodibel.api.entity import Entity, EntityMetadata, SimpleEntity
from pyodibel.api.source import Source, SourceConfig
from pyodibel.api.operations import (
    Operation,
    OperationConfig,
    EntityOperation,
    BatchOperation
)
from pyodibel.api.benchmark import (
    Benchmark,
    BenchmarkConfig,
    BenchmarkBuilder,
    BenchmarkSplit,
    SplitType
)
from pyodibel.api.evaluation import (
    Evaluator,
    EvaluationConfig,
    EvaluationResult
)

__all__ = [
    # Entity
    "Entity",
    "EntityMetadata",
    "SimpleEntity",
    # Source
    "Source",
    "SourceConfig",
    # Operations
    "Operation",
    "OperationConfig",
    "EntityOperation",
    "BatchOperation",
    # Benchmark
    "Benchmark",
    "BenchmarkConfig",
    "BenchmarkBuilder",
    "BenchmarkSplit",
    "SplitType",
    # Evaluation
    "Evaluator",
    "EvaluationConfig",
    "EvaluationResult",
]
