"""Generation config schema, canonicalization, and identity hashing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping


def content_sha256(path: str | Path | None, *, chunk_size: int = 1024 * 1024) -> str | None:
    """Return hex SHA-256 of a file, or None if path is missing/unreadable."""
    if path is None:
        return None
    file_path = Path(path)
    if not file_path.is_file():
        return None
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def content_sha256_cached(
    path: str | Path | None,
    *,
    chunk_size: int = 1024 * 1024,
    cache_suffix: str = ".sha256",
) -> str | None:
    """
    Return SHA-256 of a file, using a sidecar cache ``{path}.sha256`` when present.

    Cache is written after a full hash so large dumps are not rehashed every run.
    """
    if path is None:
        return None
    file_path = Path(path)
    if not file_path.is_file():
        return None
    cache_path = Path(str(file_path) + cache_suffix)
    if cache_path.is_file():
        cached = cache_path.read_text(encoding="utf-8").strip().split()[0]
        if len(cached) == 64 and all(char in "0123456789abcdef" for char in cached):
            return cached
    digest = content_sha256(file_path, chunk_size=chunk_size)
    if digest:
        cache_path.write_text(digest + "\n", encoding="utf-8")
    return digest


def _quantize_float(value: float, places: int = 6) -> float:
    return round(float(value), places)


@dataclass(frozen=True)
class SourceFormats:
    rdf: bool = True
    json: bool = False
    text: bool = False

    def to_dict(self) -> dict[str, bool]:
        return {"rdf": self.rdf, "json": self.json, "text": self.text}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> SourceFormats:
        data = data or {}
        return cls(
            rdf=bool(data.get("rdf", True)),
            json=bool(data.get("json", False)),
            text=bool(data.get("text", False)),
        )


@dataclass(frozen=True)
class InputsProvenance:
    """Content provenance (hashed into dataset identity)."""

    domain: str
    ontology_id: str
    ontology_content_sha256: str
    root_class: str
    dump_content_sha256: str
    dump_uri: str | None = None
    dump_version: str | None = None
    language_filter: str | None = "en"
    link_sources: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "ontology_id": self.ontology_id,
            "ontology_content_sha256": self.ontology_content_sha256,
            "root_class": self.root_class,
            "dump_content_sha256": self.dump_content_sha256,
            "dump_uri": self.dump_uri,
            "dump_version": self.dump_version,
            "language_filter": self.language_filter,
            "link_sources": list(self.link_sources),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InputsProvenance:
        links = data.get("link_sources") or []
        return cls(
            domain=str(data["domain"]),
            ontology_id=str(data["ontology_id"]),
            ontology_content_sha256=str(data["ontology_content_sha256"]),
            root_class=str(data["root_class"]),
            dump_content_sha256=str(data["dump_content_sha256"]),
            dump_uri=data.get("dump_uri"),
            dump_version=data.get("dump_version"),
            language_filter=data.get("language_filter", "en"),
            link_sources=tuple(str(item) for item in links),
        )


@dataclass(frozen=True)
class RecipeKnobs:
    """Structural recipe knobs (hashed into dataset identity)."""

    num_subsets: int = 4
    subset_size: int = 25
    overlap_ratio: float = 0.04
    target_root_budget: int = 100
    seed: int = 42
    max_hops: int | None = None
    direction: str = "forward"
    main_class_scope: str = "reachable"
    sample_method: str | None = None
    sample_n: int | None = None
    sample_fraction: float | None = None
    formats: SourceFormats = field(default_factory=SourceFormats)
    include_rdfs_label: bool = True
    include_rdf_type: bool = True
    shading_resource_ns: str = "http://kg.org/resource/"
    shading_ontology_ns: str = "http://kg.org/ontology/"
    gold_entities: bool = True
    gold_matches: bool = False
    gold_links: bool = False
    gold_relation_matches: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "num_subsets": self.num_subsets,
            "subset_size": self.subset_size,
            "overlap_ratio": _quantize_float(self.overlap_ratio),
            "target_root_budget": self.target_root_budget,
            "seed": self.seed,
            "max_hops": self.max_hops,
            "direction": self.direction,
            "main_class_scope": self.main_class_scope,
            "sample_method": self.sample_method,
            "sample_n": self.sample_n,
            "sample_fraction": (
                None
                if self.sample_fraction is None
                else _quantize_float(self.sample_fraction)
            ),
            "formats": self.formats.to_dict(),
            "include_rdfs_label": self.include_rdfs_label,
            "include_rdf_type": self.include_rdf_type,
            "shading_resource_ns": self.shading_resource_ns,
            "shading_ontology_ns": self.shading_ontology_ns,
            "gold_entities": self.gold_entities,
            "gold_matches": self.gold_matches,
            "gold_links": self.gold_links,
            "gold_relation_matches": self.gold_relation_matches,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> RecipeKnobs:
        data = dict(data or {})
        formats = SourceFormats.from_dict(data.pop("formats", None))
        known = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in data.items() if key in known}
        return cls(formats=formats, **filtered)


@dataclass(frozen=True)
class SoftwareProvenance:
    """Tooling provenance (recorded but not hashed into identity by default)."""

    pyodibel_version: str | None = None
    git_commit: str | None = None
    entrypoint: str | None = None
    stages: tuple[str, ...] = ()
    engine: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pyodibel_version": self.pyodibel_version,
            "git_commit": self.git_commit,
            "entrypoint": self.entrypoint,
            "stages": list(self.stages),
            "engine": self.engine,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> SoftwareProvenance:
        data = data or {}
        stages = data.get("stages") or []
        return cls(
            pyodibel_version=data.get("pyodibel_version"),
            git_commit=data.get("git_commit"),
            entrypoint=data.get("entrypoint"),
            stages=tuple(str(item) for item in stages),
            engine=data.get("engine"),
        )


@dataclass
class GenerationConfig:
    """Full declarative recipe for a KGI-Bench domain dataset."""

    schema_version: str
    inputs: InputsProvenance
    recipe: RecipeKnobs
    software: SoftwareProvenance = field(default_factory=SoftwareProvenance)
    parent_subgraph_id: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "inputs": self.inputs.to_dict(),
            "recipe": self.recipe.to_dict(),
            "software": self.software.to_dict(),
            "parent_subgraph_id": self.parent_subgraph_id,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> GenerationConfig:
        return cls(
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
            inputs=InputsProvenance.from_dict(data["inputs"]),
            recipe=RecipeKnobs.from_dict(data.get("recipe")),
            software=SoftwareProvenance.from_dict(data.get("software")),
            parent_subgraph_id=data.get("parent_subgraph_id"),
            notes=data.get("notes"),
        )


SCHEMA_VERSION = "1.0.0"


def canonicalize_for_hash(config: GenerationConfig | Mapping[str, Any]) -> dict[str, Any]:
    """
    Build the normalized identity payload (inputs + recipe only).

    Absolute paths must already be replaced by content hashes in ``inputs``.
    Defaults are explicit so omitted vs null hops are distinct.
    """
    if isinstance(config, GenerationConfig):
        payload = config.to_dict()
    else:
        payload = dict(config)

    inputs = InputsProvenance.from_dict(payload["inputs"]).to_dict()
    recipe = RecipeKnobs.from_dict(payload.get("recipe")).to_dict()

    # Identity excludes software, parent ids, notes, and measured outcomes.
    return {
        "schema_version": str(payload.get("schema_version", SCHEMA_VERSION)),
        "inputs": inputs,
        "recipe": recipe,
    }


def config_hash(
    config: GenerationConfig | Mapping[str, Any],
    *,
    length: int = 6,
) -> str:
    """Short SHA-256 prefix over the canonical identity JSON."""
    canonical = canonicalize_for_hash(config)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    if length < 4:
        raise ValueError("hash length must be at least 4")
    return digest[:length]


def generation_config_to_yaml_dict(config: GenerationConfig) -> dict[str, Any]:
    """Alias for serialization without importing asdict on nested frozen types."""
    return config.to_dict()
