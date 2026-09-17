"""Benchmark generation loop."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import rdflib

from kg_corrupt.config import CorruptionConfig, dump_config_yaml
from kg_corrupt.corruptions import create_strategy
from kg_corrupt.eligibility import EligibilitySets, build_eligibility_sets
from kg_corrupt.hashing import graph_sha256, json_sha256, sha256_file
from kg_corrupt.io import load_graph, to_plain_graph, write_graph, write_triples
from kg_corrupt.manifest import (
    MANIFEST_FIELDS,
    graph_id_for,
    output_stem,
    utc_now,
    write_csv,
    write_json,
    write_jsonl,
)
from kg_corrupt.model import STRATEGY_VERSION, CorruptionContext, CorruptionResult
from kg_corrupt.provenance import ProvenanceIndex, build_provenance_index, empty_provenance
from kg_corrupt.seeding import make_rng
from kg_corrupt.statistics import compute_root_stats, summarize_manifest_rows
from kg_corrupt.terms import triple_to_nt
from kg_corrupt.validation import reference_coverage, validate_corruption


@dataclass
class PreparedRoot:
    root: Any
    eligibility: EligibilitySets
    provenance: ProvenanceIndex
    root_hash: str
    provenance_hash: str | None
    root_path: Path | None


def detect_git_commit(start: Path | None = None) -> str | None:
    root = start or Path.cwd()
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def prepare_root(config: CorruptionConfig, *, root_path: Path | None = None) -> PreparedRoot:
    path = root_path or config.input.root_reference
    if path is None:
        raise ValueError("input.root_reference is required")
    loaded = load_graph(path, rdf_format=config.input.format)
    root = to_plain_graph(loaded)
    eligibility = build_eligibility_sets(root, config.eligibility)
    provenance = build_provenance_index(
        loaded,
        provenance_file=config.families.source.provenance_file,
        named_graph_as_source=config.families.source.named_graph_as_source,
    )
    if not provenance.triple_to_sources:
        provenance = empty_provenance()
    return PreparedRoot(
        root=root,
        eligibility=eligibility,
        provenance=provenance,
        root_hash=graph_sha256(root),
        provenance_hash=sha256_file(config.families.source.provenance_file),
        root_path=Path(path),
    )


def _family_context(config: CorruptionConfig, family: str, prepared: PreparedRoot) -> CorruptionContext:
    return CorruptionContext(
        protected_triples=prepared.eligibility.protected,
        family_config=config.family_config_dict(family),
        provenance=prepared.provenance,
        nested=config.benchmark.nested,
        allow_blank_seed_entities=config.eligibility.allow_blank_seed_entities,
    )


def _maybe_sidecars(
    metadata: dict[str, Any],
    stem: Path,
    *,
    inline_limit: int,
) -> dict[str, Any]:
    meta = dict(metadata)
    for key, filename in (
        ("selected_entities", "selected_entities.jsonl"),
        ("selected_predicates", "selected_predicates.jsonl"),
        ("selected_sources", "selected_sources.jsonl"),
        ("seed_entities", "seed_entities.jsonl"),
    ):
        values = meta.get(key)
        if isinstance(values, list) and len(values) > inline_limit:
            sidecar = stem.with_name(stem.name + f".{filename}")
            sidecar.write_text(
                "".join(json.dumps({"id": value}) + "\n" for value in values),
                encoding="utf-8",
            )
            meta[f"{key}_path"] = str(sidecar)
            meta[key] = values[:20]
            meta[f"{key}_truncated"] = True
    return meta


def corrupt_once(
    prepared: PreparedRoot,
    config: CorruptionConfig,
    family: str,
    fraction: float,
    seed: int,
) -> CorruptionResult:
    n_eval = prepared.eligibility.eligible_count
    target = int(round(n_eval * fraction))
    rng = make_rng(
        config.benchmark.global_seed,
        family,
        fraction,
        seed,
        nested=config.benchmark.nested,
    )
    strategy = create_strategy(family)
    root_before = canonical_snapshot(prepared.root)
    result = strategy.corrupt(
        graph=prepared.root,
        eligible_triples=prepared.eligibility.eligible,
        target_removed_count=target,
        rng=rng,
        context=_family_context(config, family, prepared),
    )
    if canonical_snapshot(prepared.root) != root_before:
        raise RuntimeError("strategy mutated the root graph")
    realized = len(result.removed_triples)
    result.metadata["reference_coverage"] = reference_coverage(realized, n_eval)
    result.metadata["target_removed_fraction"] = fraction
    result.metadata["realized_removed_fraction"] = (
        realized / n_eval if n_eval else 0.0
    )
    result.metadata["retained_eval_count"] = n_eval - realized
    validate_corruption(
        prepared.root,
        prepared.eligibility,
        result,
        target_removed_count=target,
    )
    return result


def canonical_snapshot(graph) -> str:
    return "\n".join(sorted(triple_to_nt(triple) for triple in graph))


def _reproducibility_record(
    config: CorruptionConfig,
    prepared: PreparedRoot,
    config_hash: str,
) -> dict[str, Any]:
    try:
        import kg_corrupt as pkg

        version = pkg.__version__
    except Exception:
        version = STRATEGY_VERSION
    return {
        "created_at": utc_now(),
        "git_commit": detect_git_commit(),
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "rdflib_version": rdflib.__version__,
        "strategy_version": STRATEGY_VERSION,
        "package_version": version,
        "config_hash": config_hash,
        "root_graph_hash": prepared.root_hash,
        "provenance_hash": prepared.provenance_hash,
        "nested": config.benchmark.nested,
        "global_seed": config.benchmark.global_seed,
    }


def generate_benchmark(
    config: CorruptionConfig,
    *,
    families: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    prepared = prepare_root(config)
    output_dir = Path(config.output.directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    config_hash = json_sha256(config.to_plain())
    write_json(
        output_dir / "root_stats.json",
        compute_root_stats(prepared.root, prepared.eligibility, prepared.provenance),
    )
    (output_dir / "config.yaml").write_text(dump_config_yaml(config), encoding="utf-8")
    write_json(
        output_dir / "reproducibility.json",
        _reproducibility_record(config, prepared, config_hash),
    )

    selected_families = list(families) if families else config.enabled_family_names()
    if not selected_families:
        raise ValueError("No corruption families enabled")
    if "source" in selected_families:
        prepared.provenance.require_for_source_family(prepared.eligibility.eligible)

    rows: list[dict[str, Any]] = []
    created_at = utc_now()
    n_eval = prepared.eligibility.eligible_count
    for family in selected_families:
        for fraction in config.benchmark.removal_fractions:
            for seed in config.benchmark.seeds:
                result = corrupt_once(prepared, config, family, fraction, seed)
                stem = output_stem(family, fraction, seed, output_dir)
                graph_path = stem.with_suffix(
                    ".nt" if config.output.serialization in {"nt", "ntriples"} else f".{config.output.serialization}"
                )
                meta_path = stem.with_suffix(".meta.json")
                removed_path = stem.with_name(stem.name + ".removed.nt")
                write_graph(
                    result.retained_graph,
                    graph_path,
                    serialization=config.output.serialization,
                )
                if config.output.write_removed_triples:
                    write_triples(result.removed_triples, removed_path)
                if config.output.write_retained_triples:
                    write_graph(
                        result.retained_graph,
                        stem.with_name(stem.name + ".retained.nt"),
                        serialization=config.output.serialization,
                    )
                metadata = _maybe_sidecars(
                    result.metadata,
                    stem,
                    inline_limit=config.output.inline_id_limit,
                )
                write_json(meta_path, metadata)
                realized = len(result.removed_triples)
                target = int(round(n_eval * fraction))
                row = {
                    "graph_id": graph_id_for(family, fraction, seed),
                    "family": family,
                    "seed": seed,
                    "root_graph_path": str(prepared.root_path) if prepared.root_path else "",
                    "root_triple_count": len(prepared.eligibility.all_triples),
                    "eligible_triple_count": n_eval,
                    "protected_triple_count": prepared.eligibility.protected_count,
                    "target_removed_fraction": fraction,
                    "target_removed_count": target,
                    "realized_removed_fraction": realized / n_eval if n_eval else 0.0,
                    "realized_removed_count": realized,
                    "retained_eval_count": n_eval - realized,
                    "reference_coverage": reference_coverage(realized, n_eval),
                    "output_graph_path": str(graph_path),
                    "removed_triples_path": str(removed_path)
                    if config.output.write_removed_triples
                    else "",
                    "metadata_path": str(meta_path),
                    "strategy_version": STRATEGY_VERSION,
                    "config_hash": config_hash,
                    "root_graph_hash": prepared.root_hash,
                    "created_at": created_at,
                }
                rows.append(row)

    write_csv(output_dir / "benchmark_manifest.csv", rows, MANIFEST_FIELDS)
    write_jsonl(output_dir / "benchmark_manifest.jsonl", rows)
    write_json(
        output_dir / "benchmark_summary.json",
        summarize_manifest_rows(rows),
    )
    return rows


def generate_one(
    config: CorruptionConfig,
    *,
    family: str,
    fraction: float,
    seed: int,
    output: Path,
) -> dict[str, Any]:
    prepared = prepare_root(config)
    if family == "source":
        prepared.provenance.require_for_source_family(prepared.eligibility.eligible)
    result = corrupt_once(prepared, config, family, fraction, seed)
    write_graph(result.retained_graph, output, serialization=config.output.serialization)
    meta_path = output.with_suffix(".meta.json")
    write_json(meta_path, result.metadata)
    if config.output.write_removed_triples:
        write_triples(result.removed_triples, output.with_name(output.stem + ".removed.nt"))
    n_eval = prepared.eligibility.eligible_count
    realized = len(result.removed_triples)
    return {
        "graph_id": graph_id_for(family, fraction, seed),
        "family": family,
        "seed": seed,
        "output_graph_path": str(output),
        "metadata_path": str(meta_path),
        "target_removed_fraction": fraction,
        "target_removed_count": int(round(n_eval * fraction)),
        "realized_removed_count": realized,
        "realized_removed_fraction": realized / n_eval if n_eval else 0.0,
        "reference_coverage": reference_coverage(realized, n_eval),
    }
