"""Stratified triple-level sampling."""

from __future__ import annotations

from collections import defaultdict
from typing import Literal, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.operations.sample.core import (
    Triple,
    allocate_quotas,
    relation_distribution,
    sample_stratified_groups,
    validate_sample_size,
)

StratifyBy = Literal["relation", "composite"]


def stratified_sample_triples(
    triples: Sequence[Triple],
    *,
    fraction: float | None = None,
    n: int | None = None,
    stratify_by: StratifyBy = "relation",
    seed: int = 42,
) -> list[Triple]:
    """
    Draw a stratified subset of in-memory triples.

    ``stratify_by='relation'`` groups by predicate. ``'composite'`` is not
    supported for in-memory triples because it requires rdf:type information.
    """
    if not triples:
        return []

    if stratify_by == "composite":
        raise ValueError(
            "composite stratification requires rdf:type data; use stratified_sample_rdf instead"
        )

    target_n = validate_sample_size(fraction=fraction, n=n, total=len(triples))
    by_stratum: dict[str, list[Triple]] = defaultdict(list)
    for triple in triples:
        by_stratum[triple[1]].append(triple)

    return sample_stratified_groups(by_stratum, target_n, seed)


def _entity_types_df(rdf: rDF2) -> DataFrame:
    return (
        rdf.df
        .filter(rDF2._type_filter_expr())
        .select(F.col("s").alias("entity"), F.col("o").alias("type"))
        .dropDuplicates(["entity", "type"])
    )


def _with_stratum(df: DataFrame, df_types: DataFrame, stratify_by: StratifyBy) -> DataFrame:
    if stratify_by == "relation":
        return df.withColumn("stratum", F.col("p"))

    with_source = (
        df.alias("d")
        .join(df_types.alias("ts"), F.col("d.s") == F.col("ts.entity"), "left")
        .select(
            "d.s",
            "d.p",
            "d.o",
            "d.isLiteral",
            F.coalesce(F.col("ts.type"), F.lit("Untyped")).alias("source_type"),
        )
    )

    non_literal = (
        with_source
        .filter(~F.col("isLiteral"))
        .alias("x")
        .join(df_types.alias("to"), F.col("x.o") == F.col("to.entity"), "left")
        .select(
            "x.s",
            "x.p",
            "x.o",
            "x.isLiteral",
            F.concat_ws(
                "|",
                F.col("x.source_type"),
                F.col("x.p"),
                F.coalesce(F.col("to.type"), F.lit("Untyped")),
            ).alias("stratum"),
        )
    )

    literal = with_source.filter(F.col("isLiteral")).select(
        "s",
        "p",
        "o",
        "isLiteral",
        F.concat_ws("|", F.col("source_type"), F.col("p"), F.lit("Literal")).alias("stratum"),
    )

    return non_literal.unionByName(literal)


def _sample_stratified_df(df: DataFrame, target_n: int, seed: int) -> DataFrame:
    total = df.count()
    if target_n >= total:
        return df.select("s", "p", "o", "isLiteral")

    counts = {
        row["stratum"]: row["count"]
        for row in df.groupBy("stratum").count().collect()
    }
    quotas = allocate_quotas(counts, target_n)
    quota_rows = [(stratum, quota) for stratum, quota in quotas.items() if quota > 0]
    quota_df = df.sparkSession.createDataFrame(quota_rows, ["stratum", "quota"])

    ranked = df.withColumn(
        "rn",
        F.row_number().over(Window.partitionBy("stratum").orderBy(F.rand(seed))),
    )

    return (
        ranked.alias("r")
        .join(quota_df.alias("q"), F.col("r.stratum") == F.col("q.stratum"), "inner")
        .filter(F.col("r.rn") <= F.col("q.quota"))
        .select("r.s", "r.p", "r.o", "r.isLiteral")
    )


def stratified_sample_rdf(
    rdf: rDF2,
    *,
    fraction: float | None = None,
    n: int | None = None,
    stratify_by: StratifyBy = "relation",
    seed: int = 42,
) -> rDF2:
    """Stratified downsampling for an ``rDF2`` instance."""
    total = rdf.df.count()
    if total == 0:
        return rDF2(rdf.df.limit(0))

    target_n = validate_sample_size(fraction=fraction, n=n, total=total)

    if stratify_by == "composite":
        with_stratum = _with_stratum(rdf.df, _entity_types_df(rdf), stratify_by)
    else:
        with_stratum = rdf.df.withColumn("stratum", F.col("p"))

    sampled_df = _sample_stratified_df(with_stratum, target_n, seed)
    return rDF2(sampled_df)
