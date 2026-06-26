"""Distribution summaries for comparing original and sampled graphs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pyspark.sql import functions as F

from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.operations.sample.subgraph import (
    DEFAULT_DEGREE_BINS,
    DegreeMetric,
    entity_features_df,
)

DistributionKind = Literal["class", "degree", "degree_bin", "property"]


@dataclass(frozen=True)
class DistributionComparison:
    kind: DistributionKind
    original: dict[str, int]
    sampled: dict[str, int]

    @property
    def original_total(self) -> int:
        return sum(self.original.values())

    @property
    def sampled_total(self) -> int:
        return sum(self.sampled.values())

    def max_abs_frequency_delta(self) -> float:
        labels = set(self.original) | set(self.sampled)
        if not labels:
            return 0.0
        deltas = []
        for label in labels:
            orig_frac = self.original.get(label, 0) / max(self.original_total, 1)
            samp_frac = self.sampled.get(label, 0) / max(self.sampled_total, 1)
            deltas.append(abs(orig_frac - samp_frac))
        return max(deltas)


def class_distribution(rdf: rDF2) -> dict[str, int]:
    rows = (
        rdf.df
        .filter(rDF2._type_filter_expr())
        .groupBy(F.col("o").alias("label"))
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    return {row["label"]: int(row["count"]) for row in rows}


def property_distribution(rdf: rDF2) -> dict[str, int]:
    rows = (
        rdf.df
        .groupBy(F.col("p").alias("label"))
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    return {row["label"]: int(row["count"]) for row in rows}


def degree_distribution(
    rdf: rDF2,
    *,
    degree_metric: DegreeMetric = "out_triples",
) -> dict[str, int]:
    features = entity_features_df(rdf, degree_metric=degree_metric)
    rows = (
        features
        .groupBy(F.col("degree").cast("string").alias("label"))
        .count()
        .orderBy(F.asc("label"))
        .collect()
    )
    return {row["label"]: int(row["count"]) for row in rows}


def degree_bin_distribution(
    rdf: rDF2,
    *,
    degree_metric: DegreeMetric = "out_triples",
    degree_bins: tuple[int, ...] = DEFAULT_DEGREE_BINS,
) -> dict[str, int]:
    features = entity_features_df(
        rdf,
        degree_metric=degree_metric,
        degree_bins=degree_bins,
    )
    rows = (
        features
        .groupBy("degree_bin")
        .count()
        .orderBy(F.asc("degree_bin"))
        .collect()
    )
    return {row["degree_bin"]: int(row["count"]) for row in rows}


def compare_distributions(
    original: rDF2,
    sampled: rDF2,
    *,
    degree_metric: DegreeMetric = "out_triples",
    degree_bins: tuple[int, ...] = DEFAULT_DEGREE_BINS,
) -> list[DistributionComparison]:
    return [
        DistributionComparison("class", class_distribution(original), class_distribution(sampled)),
        DistributionComparison(
            "degree_bin",
            degree_bin_distribution(
                original,
                degree_metric=degree_metric,
                degree_bins=degree_bins,
            ),
            degree_bin_distribution(
                sampled,
                degree_metric=degree_metric,
                degree_bins=degree_bins,
            ),
        ),
        DistributionComparison(
            "property",
            property_distribution(original),
            property_distribution(sampled),
        ),
    ]


def format_distribution_report(comparisons: list[DistributionComparison]) -> str:
    lines = ["Distribution comparison (max abs frequency delta per kind):"]
    for comparison in comparisons:
        lines.append(
            f"  {comparison.kind}: {comparison.max_abs_frequency_delta():.3%} "
            f"(original={comparison.original_total:,}, sampled={comparison.sampled_total:,})"
        )
    return "\n".join(lines)
