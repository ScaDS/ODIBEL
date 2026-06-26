"""Representative subgraph sampling by entity class and degree."""

from __future__ import annotations

from typing import Literal, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.operations.sample.core import allocate_quotas, validate_sample_size

DegreeMetric = Literal["out_triples", "out_predicates"]
DEFAULT_DEGREE_BINS: tuple[int, ...] = (5, 20, 100)


def degree_bin_label(degree: int, bins: Sequence[int] = DEFAULT_DEGREE_BINS) -> str:
    """Map a positive degree to a human-readable bin label."""
    if degree <= 0:
        return "0"
    lower = 1
    for upper in bins:
        if degree <= upper:
            return f"{lower}-{upper}"
        lower = upper + 1
    return f"{lower}+"


def _degree_bin_expr(degree_col: F.Column, bins: Sequence[int]) -> F.Column:
    expr = F.when(degree_col <= 0, F.lit("0"))
    lower = 1
    for upper in bins:
        expr = expr.when(
            (degree_col >= lower) & (degree_col <= upper),
            F.lit(f"{lower}-{upper}"),
        )
        lower = upper + 1
    return expr.otherwise(F.lit(f"{lower}+"))


def _entity_degree_df(rdf: rDF2, degree_metric: DegreeMetric) -> DataFrame:
    grouped = rdf.df.groupBy(F.col("s").alias("entity"))
    if degree_metric == "out_predicates":
        return grouped.agg(F.countDistinct("p").alias("degree"))
    return grouped.agg(F.count("*").alias("degree"))


def _entity_primary_types_df(rdf: rDF2) -> DataFrame:
    df_types = (
        rdf.df
        .filter(rDF2._type_filter_expr())
        .select(F.col("s").alias("entity"), F.col("o").alias("type"))
        .dropDuplicates(["entity", "type"])
    )
    return df_types.groupBy("entity").agg(F.min("type").alias("type"))


def entity_features_df(
    rdf: rDF2,
    *,
    degree_metric: DegreeMetric = "out_triples",
    degree_bins: Sequence[int] = DEFAULT_DEGREE_BINS,
) -> DataFrame:
    """Per-entity class, degree, and joint stratum used for subgraph sampling."""
    degrees = _entity_degree_df(rdf, degree_metric)
    types = _entity_primary_types_df(rdf)

    return (
        degrees.alias("d")
        .join(types.alias("t"), F.col("d.entity") == F.col("t.entity"), "left")
        .select(
            F.col("d.entity").alias("entity"),
            F.coalesce(F.col("t.type"), F.lit("Untyped")).alias("type"),
            F.col("d.degree").alias("degree"),
            _degree_bin_expr(F.col("d.degree"), degree_bins).alias("degree_bin"),
        )
        .withColumn("stratum", F.concat_ws("|", F.col("type"), F.col("degree_bin")))
    )


def _sample_entities_stratified(features: DataFrame, target_n: int, seed: int) -> DataFrame:
    total = features.count()
    if target_n >= total:
        return features.select("entity").dropDuplicates(["entity"])

    counts = {
        row["stratum"]: row["count"]
        for row in features.groupBy("stratum").count().collect()
    }
    quotas = allocate_quotas(counts, target_n)
    quota_rows = [(stratum, quota) for stratum, quota in quotas.items() if quota > 0]
    quota_df = features.sparkSession.createDataFrame(quota_rows, ["stratum", "quota"])

    ranked = features.withColumn(
        "rn",
        F.row_number().over(Window.partitionBy("stratum").orderBy(F.rand(seed))),
    )

    return (
        ranked.alias("r")
        .join(quota_df.alias("q"), F.col("r.stratum") == F.col("q.stratum"), "inner")
        .filter(F.col("r.rn") <= F.col("q.quota"))
        .select("r.entity")
        .dropDuplicates(["entity"])
    )


def induce_entity_subgraph(rdf: rDF2, selected_entities: DataFrame) -> rDF2:
    """Keep all triples whose subject is in ``selected_entities``."""
    sampled_df = (
        rdf.df.alias("d")
        .join(selected_entities.alias("sel"), F.col("d.s") == F.col("sel.entity"), "inner")
        .select("d.s", "d.p", "d.o", "d.isLiteral")
    )
    return rDF2(sampled_df)


def sample_representative_subgraph(
    rdf: rDF2,
    *,
    fraction: float | None = None,
    n: int | None = None,
    degree_metric: DegreeMetric = "out_triples",
    degree_bins: Sequence[int] = DEFAULT_DEGREE_BINS,
    seed: int = 42,
) -> rDF2:
    """
    Build a subgraph with similar class and degree distributions.

    Entities are sampled stratified on ``(rdf:type, degree_bin)``. The induced
    subgraph contains all outgoing triples for selected entities.
    """
    features = entity_features_df(
        rdf,
        degree_metric=degree_metric,
        degree_bins=degree_bins,
    )
    entity_count = features.select("entity").dropDuplicates(["entity"]).count()
    if entity_count == 0:
        return rDF2(rdf.df.limit(0))

    target_n = validate_sample_size(fraction=fraction, n=n, total=entity_count)
    selected = _sample_entities_stratified(features, target_n, seed)
    return induce_entity_subgraph(rdf, selected)
