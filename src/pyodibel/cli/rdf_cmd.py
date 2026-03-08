"""
RDF command group for PyODIBEL CLI.

Provides thin CLI wrappers around RDF pipeline operations implemented in
`pyodibel.operations.rdf.rdf2`.
"""

import click

from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2


def _normalize_uri(uri: str) -> str:
    """Accept raw URI or <URI> and return N-Triples URI form."""
    value = uri.strip()
    if value.startswith("<") and value.endswith(">"):
        return value
    return f"<{value}>"


def _parse_type_targets(raw_targets: tuple[str, ...]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for raw in raw_targets:
        if "=" not in raw:
            raise click.BadParameter(
                f"Invalid --type-target '{raw}'. Expected format: TYPE_URI=COUNT"
            )
        raw_type, raw_count = raw.split("=", 1)
        entity_type = _normalize_uri(raw_type)
        try:
            target_count = int(raw_count)
        except ValueError as exc:
            raise click.BadParameter(
                f"Invalid count in --type-target '{raw}'. COUNT must be an integer."
            ) from exc
        if target_count < 0:
            raise click.BadParameter(
                f"Invalid count in --type-target '{raw}'. COUNT must be >= 0."
            )
        parsed[entity_type] = target_count
    return parsed


@click.group("rdf")
def rdf_group():
    """Run RDF pipelines."""


@rdf_group.command("run")
@click.option("--input", "input_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--output", "output_path", required=True, type=click.Path())
@click.option(
    "--filter-s-type",
    "subject_types",
    multiple=True,
    help="Keep triples whose subject is of this rdf:type. Repeat to chain multiple filters.",
)
@click.option(
    "--type-target",
    "type_targets",
    multiple=True,
    help=(
        "Sample target per type in the form TYPE_URI=COUNT. "
        "Can be repeated and runs rarity-first iterative sampling."
    ),
)
@click.option(
    "--global-sample-size",
    default=None,
    type=int,
    help="Global number of entities to sample, independent of type.",
)
@click.option(
    "--all-types-target",
    default=None,
    type=int,
    help="Uniform per-type target for all discovered rdf:types (e.g., 5 means up to 5 per type).",
)
@click.option(
    "--related-per-seed",
    default=None,
    type=int,
    help=(
        "Maximum number of directly related entities added per newly sampled entity. "
        "Default is 5, but 0 for --all-types-target unless explicitly provided."
    ),
)
@click.option("--sample-seed", default=13, show_default=True, type=int)
@click.option("--app-name", default="RDFPipeline", show_default=True)
@click.option("--master", default="local[*]", show_default=True)
@click.option("--driver-memory", default="8g", show_default=True)
@click.option(
    "--executor-memory",
    default="8g",
    show_default=True,
    help="Only relevant for non-local Spark masters.",
)
@click.option("--shuffle-partitions", default=210, show_default=True, type=int)
@click.option("--local-dir", default="/tmp/spark", show_default=True)
@click.option("--adaptive-enabled/--no-adaptive-enabled", default=True, show_default=True)
@click.option("--skew-join-enabled/--no-skew-join-enabled", default=True, show_default=True)
def run_pipeline(
    input_path: str,
    output_path: str,
    subject_types: tuple[str, ...],
    type_targets: tuple[str, ...],
    global_sample_size: int | None,
    all_types_target: int | None,
    related_per_seed: int | None,
    sample_seed: int,
    app_name: str,
    master: str,
    driver_memory: str,
    executor_memory: str,
    shuffle_partitions: int,
    local_dir: str,
    adaptive_enabled: bool,
    skew_join_enabled: bool,
):
    """Execute RDF pipeline: parse -> filters -> write."""
    if related_per_seed is not None and related_per_seed < 0:
        raise click.BadParameter("--related-per-seed must be >= 0")

    if global_sample_size is not None and global_sample_size < 0:
        raise click.BadParameter("--global-sample-size must be >= 0")
    if all_types_target is not None and all_types_target < 0:
        raise click.BadParameter("--all-types-target must be >= 0")

    mode_count = (
        int(bool(subject_types))
        + int(bool(type_targets))
        + int(global_sample_size is not None)
        + int(all_types_target is not None)
    )
    if mode_count > 1:
        raise click.UsageError(
            "Use only one sampling mode: --filter-s-type OR --type-target OR --global-sample-size OR --all-types-target."
        )

    effective_related_per_seed = related_per_seed
    if effective_related_per_seed is None:
        effective_related_per_seed = 0 if all_types_target is not None else 5

    spark = get_spark_session(
        app_name=app_name,
        master=master,
        executor_memory=executor_memory,
        driver_memory=driver_memory,
        shuffle_partitions=shuffle_partitions,
        local_dir=local_dir,
        adaptive_enabled=adaptive_enabled,
        skew_join_enabled=skew_join_enabled,
    )

    try:
        pipeline = rDF2.parse(spark, input_path)

        if all_types_target is not None:
            pipeline = pipeline.sample_entities_all_types(
                target_per_type=all_types_target,
                related_per_seed=effective_related_per_seed,
                seed=sample_seed,
            )
        elif global_sample_size is not None:
            pipeline = pipeline.sample_entities_global(
                sample_size=global_sample_size,
                related_per_seed=effective_related_per_seed,
                seed=sample_seed,
            )
        elif type_targets:
            parsed_targets = _parse_type_targets(type_targets)
            pipeline = pipeline.sample_entities_by_type_targets(
                type_targets=parsed_targets,
                related_per_seed=effective_related_per_seed,
                seed=sample_seed,
            )
        else:
            for subject_type in subject_types:
                pipeline = pipeline.filter_triples_by_s_type(_normalize_uri(subject_type))

        pipeline.write_nt(output_path)
        click.echo(f"Wrote RDF output to: {output_path}")
    finally:
        spark.stop()
