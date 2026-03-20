"""
Compatibility wrappers for schema-graph operations.

Preferred implementation now lives on `pyodibel.operations.rdf.rdf2.rDF2`.
"""

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession

from pyodibel.operations.rdf.rdf2 import rDF2


def build_schema_graph_df(
    triples: DataFrame,
    property_filters: Iterable[str] | None = None,
) -> DataFrame:
    return rDF2(triples).build_schema_graph_df(property_filters=property_filters)


def build_schema_graph(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    property_filters: Iterable[str] | None = None,
) -> None:
    """Parse N-Triples and write schema graph statistics as CSV."""
    rDF2.parse(spark, input_path).write_schema_graph_csv(
        output_path,
        property_filters=property_filters,
    )
