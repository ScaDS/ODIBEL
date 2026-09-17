"""Helpers to assemble GenerationConfig from filesystem inputs."""

from __future__ import annotations

import subprocess
from importlib import metadata
from pathlib import Path
from typing import Sequence

from pyodibel.datasets.kgi_provenance.config import (
    SCHEMA_VERSION,
    GenerationConfig,
    InputsProvenance,
    RecipeKnobs,
    SoftwareProvenance,
    SourceFormats,
    content_sha256,
    content_sha256_cached,
)


def detect_git_commit(cwd: str | Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd) if cwd else None,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or None
    except (OSError, subprocess.CalledProcessError):
        return None


def detect_pyodibel_version() -> str | None:
    try:
        return metadata.version("pyodibel")
    except metadata.PackageNotFoundError:
        return None


def build_generation_config(
    *,
    domain: str,
    ontology_id: str,
    ontology_path: str | Path,
    root_class: str,
    dump_path: str | Path,
    dump_uri: str | None = None,
    dump_version: str | None = None,
    language_filter: str | None = "en",
    link_sources: Sequence[str] = (),
    recipe: RecipeKnobs | None = None,
    software: SoftwareProvenance | None = None,
    parent_subgraph_id: str | None = None,
    notes: str | None = None,
    ontology_content_sha256: str | None = None,
    dump_content_sha256: str | None = None,
) -> GenerationConfig:
    """Create a GenerationConfig, hashing ontology/dump files when checksums omitted."""
    ontology_hash = ontology_content_sha256 or content_sha256(ontology_path)
    dump_hash = dump_content_sha256 or content_sha256_cached(dump_path)
    if not ontology_hash:
        raise FileNotFoundError(f"ontology file not found or unreadable: {ontology_path}")
    if not dump_hash:
        raise FileNotFoundError(f"dump file not found or unreadable: {dump_path}")

    resolved_software = software or SoftwareProvenance(
        pyodibel_version=detect_pyodibel_version(),
        git_commit=detect_git_commit(),
        entrypoint="resources/text2kgbench/run_domain.py",
    )

    return GenerationConfig(
        schema_version=SCHEMA_VERSION,
        inputs=InputsProvenance(
            domain=domain,
            ontology_id=ontology_id,
            ontology_content_sha256=ontology_hash,
            root_class=root_class,
            dump_content_sha256=dump_hash,
            dump_uri=dump_uri or f"file://{Path(dump_path).resolve()}",
            dump_version=dump_version,
            language_filter=language_filter,
            link_sources=tuple(link_sources),
        ),
        recipe=recipe or RecipeKnobs(),
        software=resolved_software,
        parent_subgraph_id=parent_subgraph_id,
        notes=notes,
    )


def default_rdf_only_recipe(
    *,
    target_root_budget: int,
    subset_size: int,
    num_subsets: int = 4,
    overlap_ratio: float = 0.04,
    seed: int = 42,
    max_hops: int | None = None,
    direction: str = "forward",
    main_class_scope: str = "reachable",
) -> RecipeKnobs:
    return RecipeKnobs(
        num_subsets=num_subsets,
        subset_size=subset_size,
        overlap_ratio=overlap_ratio,
        target_root_budget=target_root_budget,
        seed=seed,
        max_hops=max_hops,
        direction=direction,
        main_class_scope=main_class_scope,
        formats=SourceFormats(rdf=True, json=False, text=False),
        gold_entities=True,
        gold_matches=False,
        gold_links=False,
        gold_relation_matches=False,
    )
