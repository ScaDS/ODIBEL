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


def _parse_classes(raw_classes: tuple[str, ...], classes_csv: str | None) -> tuple[str, ...]:
    values = list(raw_classes)
    if classes_csv:
        values.extend(part.strip() for part in classes_csv.split(","))

    normalized: list[str] = []
    for raw in values:
        if not raw or not raw.strip():
            continue
        normalized.append(_normalize_uri(raw))

    return tuple(dict.fromkeys(normalized))


def _normalize_property_filter(raw_filter: str) -> str:
    """
    Normalize predicate filter.

    - exact: URI -> <URI>
    - prefix: URI* -> <URI*
    - shorthand token: TOKEN -> <TOKEN*
    """
    value = raw_filter.strip()
    if not value:
        raise click.BadParameter("--property-filter values must not be empty")

    if value.endswith("*"):
        base = value[:-1].strip()
        if not base:
            raise click.BadParameter("Invalid --property-filter '*'. Missing prefix.")
        if base.startswith("<"):
            normalized = base
        else:
            normalized = f"<{base}"
        if normalized.endswith(">"):
            normalized = normalized[:-1]
        return f"{normalized}*"

    if value.startswith("<") and value.endswith(">"):
        return value

    if "://" in value:
        return _normalize_uri(value)

    # Shorthand token (for example "http"): interpret as prefix.
    token = value
    if token.startswith("<"):
        token = token[1:]
    if token.endswith(">"):
        token = token[:-1]
    token = token.strip()
    if not token:
        raise click.BadParameter(f"Invalid --property-filter '{raw_filter}'.")
    return f"<{token}*"


def _parse_property_filters(raw_filters: tuple[str, ...]) -> tuple[str, ...]:
    if not raw_filters:
        return tuple()
    normalized = [_normalize_property_filter(raw_filter) for raw_filter in raw_filters]
    return tuple(dict.fromkeys(normalized))


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
    "--class",
    "classes",
    multiple=True,
    help="Allowed class URI. Repeat this option for multiple classes.",
)
@click.option(
    "--classes",
    "classes_csv",
    default=None,
    help="Comma-separated allowed class URIs, e.g. URI1,URI2,URI3.",
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
    classes: tuple[str, ...],
    classes_csv: str | None,
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
    parsed_classes = _parse_classes(classes, classes_csv)

    if related_per_seed is not None and related_per_seed < 0:
        raise click.BadParameter("--related-per-seed must be >= 0")

    if global_sample_size is not None and global_sample_size < 0:
        raise click.BadParameter("--global-sample-size must be >= 0")
    if all_types_target is not None and all_types_target < 0:
        raise click.BadParameter("--all-types-target must be >= 0")

    mode_count = (
        int(bool(subject_types))
        + int(bool(type_targets))
        + int(bool(parsed_classes))
        + int(global_sample_size is not None)
        + int(all_types_target is not None)
    )
    if mode_count > 1:
        raise click.UsageError(
            "Use only one sampling mode: --filter-s-type OR --type-target OR --class/--classes OR --global-sample-size OR --all-types-target."
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
        elif parsed_classes:
            pipeline = pipeline.filter_subgraph_by_entity_classes(list(parsed_classes))
        else:
            for subject_type in subject_types:
                pipeline = pipeline.filter_triples_by_s_type(_normalize_uri(subject_type))

        pipeline.write_nt(output_path)
        click.echo(f"Wrote RDF output to: {output_path}")
    finally:
        spark.stop()


@rdf_group.command("schema-graph")
@click.option("--input", "input_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--output", "output_path", required=True, type=click.Path())
@click.option("--app-name", default="SchemaGraphGenerator", show_default=True)
@click.option("--master", default="local[*]", show_default=True)
@click.option("--driver-memory", default="8g", show_default=True)
@click.option(
    "--executor-memory",
    default="8g",
    show_default=True,
    help="Only relevant for non-local Spark masters.",
)
@click.option("--shuffle-partitions", default=2000, show_default=True, type=int)
@click.option("--local-dir", default="/tmp/spark", show_default=True)
@click.option("--adaptive-enabled/--no-adaptive-enabled", default=True, show_default=True)
@click.option("--skew-join-enabled/--no-skew-join-enabled", default=True, show_default=True)
@click.option(
    "--property-filter",
    "property_filters",
    multiple=True,
    help=(
        "Predicate filter for schema graph. Repeat option to include more filters. "
        "Use URI for exact match, URI* for prefix match, or a shorthand token (e.g. http) for prefix match."
    ),
)
def build_schema_graph_cmd(
    input_path: str,
    output_path: str,
    app_name: str,
    master: str,
    driver_memory: str,
    executor_memory: str,
    shuffle_partitions: int,
    local_dir: str,
    adaptive_enabled: bool,
    skew_join_enabled: bool,
    property_filters: tuple[str, ...],
):
    """Build a schema-level SourceType-Relation-TargetType graph CSV from N-Triples."""
    parsed_property_filters = _parse_property_filters(property_filters)

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
        rDF2.parse(spark, input_path).write_schema_graph_csv(
            output_path,
            property_filters=parsed_property_filters,
        )
        click.echo(f"Wrote schema graph CSV to: {output_path}")
    finally:
        spark.stop()
