"""
Storage backend implementations.

This module provides implementations for different storage backends:
- File-based storage (local or network file systems)
- Database storage (SQL databases via SQLAlchemy)
- Object storage (S3-compatible storage)
"""

from pyodibel.storage.spark_backend import SparkBackend

__all__ = [
    "SparkBackend",
]
