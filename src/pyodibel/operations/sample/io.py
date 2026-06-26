"""File-based sampling entry points using Spark."""

from __future__ import annotations

import os
from typing import Literal, Sequence

from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.operations.sample.distributions import compare_distributions, format_distribution_report
from pyodibel.operations.sample.stratified import StratifyBy, stratified_sample_rdf
from pyodibel.operations.sample.subgraph import (
    DEFAULT_DEGREE_BINS,
    DegreeMetric,
    sample_representative_subgraph,
)

SampleMethod = Literal["stratified", "subgraph"]


def sample_nt_file(
    input_path: str,
    output_path: str,
    *,
    method: SampleMethod = "subgraph",
    fraction: float | None = None,
    n: int | None = None,
    stratify_by: StratifyBy = "relation",
    degree_metric: DegreeMetric = "out_triples",
    degree_bins: Sequence[int] = DEFAULT_DEGREE_BINS,
    seed: int = 42,
    master: str | None = None,
    report: bool = True,
) -> None:
    """Read NT triples, sample them, optionally report distributions, and write NT."""
    spark = get_spark_session("PyODIBELSampler", master=master)
    try:
        rdf = rDF2.parse(spark, input_path)
        before_entities = rdf.df.select("s").distinct().count()
        before_triples = rdf.df.count()

        if method == "stratified":
            sampled = stratified_sample_rdf(
                rdf,
                fraction=fraction,
                n=n,
                stratify_by=stratify_by,
                seed=seed,
            )
        elif method == "subgraph":
            sampled = sample_representative_subgraph(
                rdf,
                fraction=fraction,
                n=n,
                degree_metric=degree_metric,
                degree_bins=degree_bins,
                seed=seed,
            )
        else:
            raise ValueError(f"Unknown sampling method: {method}")

        after_entities = sampled.df.select("s").distinct().count()
        after_triples = sampled.df.count()

        if os.path.exists(output_path):
            raise ValueError(f"output path already exists: {output_path}")

        sampled.write_nt(output_path)

        print(f"Method:          {method}")
        print(f"Input entities:  {before_entities:,}")
        print(f"Input triples:   {before_triples:,}")
        print(f"Output entities: {after_entities:,}")
        print(f"Output triples:  {after_triples:,}")
        if before_triples:
            print(f"Triple ratio:    {after_triples / before_triples:.1%}")

        if report:
            print()
            print(format_distribution_report(compare_distributions(rdf, sampled)))
        print(f"Wrote:           {output_path}")
    finally:
        spark.stop()
