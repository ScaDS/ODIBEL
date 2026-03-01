"""
Gradoop data source implementation.

Provides GradoopDataSource for loading entity resolution results from
Gradoop/FAMER exports, and GradoopEntity for representing entities.
"""

from pyodibel.source.gradoop.source import GradoopDataSource
from pyodibel.source.gradoop.entity import GradoopEntity

__all__ = [
    "GradoopDataSource",
    "GradoopEntity",
]



