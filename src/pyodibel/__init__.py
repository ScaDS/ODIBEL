"""
PyODIBEL: Open Data Integration Benchmark Evaluation Lab

A library for generating and evaluating benchmark datasets for data integration tasks
across different domains, built from Linked Open Data (LOD) sources and large public dumps.

The top-level package re-exports the experimental ``pyodibel.api`` interfaces only.
Import everything else from its own module (see docs/api.md). Importing the package
has no side effects: it does not configure logging or write files.
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
    # Logging (opt-in)
    "setup_logging",
]


def setup_logging(log_file='pyodibel.log', level=logging.INFO):
    """
    Opt in to file logging for PyODIBEL.

    Importing ``pyodibel`` does not call this and never creates a log file.
    Call it explicitly from an application or script that wants one.

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
