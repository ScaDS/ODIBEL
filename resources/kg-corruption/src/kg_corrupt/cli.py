"""Command-line interface for the corruption benchmark generator."""

from __future__ import annotations

from pathlib import Path

import click

from kg_corrupt.config import CorruptionConfig, load_config
from kg_corrupt.generator import generate_benchmark, generate_one, prepare_root
from kg_corrupt.io import load_graph, to_plain_graph
from kg_corrupt.manifest import read_manifest, write_json
from kg_corrupt.provenance import ProvenanceError
from kg_corrupt.statistics import compute_root_stats, summarize_manifest_rows
from kg_corrupt.validation import ValidationError, validate_corruption


def _apply_overrides(
    config: CorruptionConfig,
    *,
    root_reference: Path | None = None,
    provenance: Path | None = None,
    output_directory: Path | None = None,
) -> CorruptionConfig:
    if root_reference is not None:
        config.input.root_reference = Path(root_reference)
    if provenance is not None:
        config.families.source.provenance_file = Path(provenance)
        config.families.source.enabled = True
    if output_directory is not None:
        config.output.directory = Path(output_directory)
    return config


@click.group()
def cli():
    """Controlled omission-only KG corruption benchmark."""


@cli.command("generate")
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--family", "families", multiple=True, help="Restrict generation to one or more families.")
@click.option("--root-reference", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--provenance", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--output", type=click.Path(file_okay=False, path_type=Path))
def generate_cmd(config_path, families, root_reference, provenance, output):
    """Generate the full (family × fraction × seed) benchmark."""
    config = _apply_overrides(
        load_config(config_path),
        root_reference=root_reference,
        provenance=provenance,
        output_directory=output,
    )
    selected = list(families) or None
    try:
        rows = generate_benchmark(config, families=selected)
    except ProvenanceError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Wrote {len(rows)} graphs to {config.output.directory}")


@cli.command("generate-one")
@click.option("--root-reference", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--family", required=True)
@click.option("--remove-fraction", type=float, required=True)
@click.option("--seed", type=int, required=True)
@click.option("--output", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--provenance", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def generate_one_cmd(root_reference, family, remove_fraction, seed, output, config_path, provenance):
    """Generate a single corrupted graph."""
    config = load_config(config_path) if config_path else CorruptionConfig()
    config = _apply_overrides(
        config,
        root_reference=root_reference,
        provenance=provenance,
    )
    try:
        row = generate_one(
            config,
            family=family,
            fraction=remove_fraction,
            seed=seed,
            output=Path(output),
        )
    except ProvenanceError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"{row['graph_id']} coverage={row['reference_coverage']:.6f} "
        f"removed={row['realized_removed_count']}"
    )


@cli.command("stats")
@click.option("--root-reference", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--provenance", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--output", type=click.Path(dir_okay=False, path_type=Path))
def stats_cmd(root_reference, config_path, provenance, output):
    """Print or write root-graph statistics."""
    config = load_config(config_path) if config_path else CorruptionConfig()
    config = _apply_overrides(config, root_reference=root_reference, provenance=provenance)
    prepared = prepare_root(config)
    stats = compute_root_stats(prepared.root, prepared.eligibility, prepared.provenance)
    if output:
        write_json(Path(output), stats)
        click.echo(str(output))
    else:
        click.echo(
            "\n".join(
                [
                    f"total triples: {stats['total_triples']}",
                    f"eligible triples: {stats['eligible_triples']}",
                    f"protected triples: {stats['protected_triples']}",
                    f"unique subjects: {stats['unique_subjects']}",
                    f"unique resource objects: {stats['unique_resource_objects']}",
                    f"unique entities: {stats['unique_entities']}",
                    f"unique predicates: {stats['unique_predicates']}",
                    f"source units: {stats.get('source_unit_count', 0)}",
                ]
            )
        )


@cli.command("validate")
@click.option("--manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
def validate_cmd(manifest):
    """Re-check subset, protection, and coverage invariants for a generated run."""
    rows = read_manifest(Path(manifest))
    if not rows:
        raise click.ClickException("manifest is empty")
    root_path = Path(rows[0]["root_graph_path"])
    root = to_plain_graph(load_graph(root_path))
    from kg_corrupt.eligibility import build_eligibility_sets as _elig

    # Eligibility is reconstructed from the stored counts plus re-loading config if present.
    config_path = Path(manifest).parent / "config.yaml"
    config = load_config(config_path) if config_path.is_file() else CorruptionConfig()
    eligibility = _elig(root, config.eligibility)
    errors = 0
    for row in rows:
        graph = to_plain_graph(load_graph(row["output_graph_path"]))
        removed_path = row.get("removed_triples_path") or ""
        if removed_path and Path(removed_path).is_file():
            removed_graph = to_plain_graph(load_graph(removed_path))
            removed = set(removed_graph)
        else:
            removed = set(eligibility.eligible) - set(graph)
        from kg_corrupt.model import CorruptionResult

        result = CorruptionResult(
            retained_graph=graph,
            removed_triples=removed,
            metadata={
                "realized_removed_count": int(float(row["realized_removed_count"])),
                "reference_coverage": float(row["reference_coverage"]),
            },
        )
        try:
            validate_corruption(root, eligibility, result)
        except ValidationError as exc:
            click.echo(f"{row['graph_id']}: {exc}", err=True)
            errors += 1
            continue
        measured_removed = len(set(eligibility.eligible) - set(graph))
        if measured_removed != int(float(row["realized_removed_count"])):
            click.echo(
                f"{row['graph_id']}: manifest realized_removed_count "
                f"{row['realized_removed_count']} != measured {measured_removed}",
                err=True,
            )
            errors += 1
    if errors:
        raise click.ClickException(f"{errors} graph(s) failed validation")
    click.echo(f"Validated {len(rows)} graphs")


@cli.command("summarize")
@click.option("--manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--output", type=click.Path(dir_okay=False, path_type=Path))
def summarize_cmd(manifest, output):
    """Summarize realized corruption by family and target level."""
    rows = read_manifest(Path(manifest))
    summary = summarize_manifest_rows(rows)
    if output:
        write_json(Path(output), summary)
        click.echo(str(output))
        return
    for item in summary:
        click.echo(
            f"{item['family']:12} {item['target_removed_fraction']:.2f}  "
            f"n={item['n']:3}  mean={item['mean_realized_corruption']:.4f}  "
            f"std={item['std_realized_corruption']:.4f}"
        )


if __name__ == "__main__":
    cli()
