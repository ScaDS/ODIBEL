"""YAML configuration with documented defaults."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping

import yaml

DEFAULT_PROTECTED_PREDICATES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
)

DEFAULT_REMOVAL_FRACTIONS = (0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70)
DEFAULT_SEEDS = tuple(range(1, 11))


@dataclass
class InputConfig:
    root_reference: Path | None = None
    format: str | None = None


@dataclass
class OutputConfig:
    directory: Path = Path("outputs/corruptions")
    serialization: str = "nt"
    write_removed_triples: bool = True
    write_retained_triples: bool = False
    inline_id_limit: int = 200


@dataclass
class BenchmarkConfig:
    seeds: list[int] = field(default_factory=lambda: list(DEFAULT_SEEDS))
    removal_fractions: list[float] = field(
        default_factory=lambda: list(DEFAULT_REMOVAL_FRACTIONS)
    )
    nested: bool = False
    global_seed: int = 0


@dataclass
class EligibilityConfig:
    protected_predicates: list[str] = field(
        default_factory=lambda: list(DEFAULT_PROTECTED_PREDICATES)
    )
    protected_namespaces: list[str] = field(default_factory=list)
    protect_schema_triples: bool = True
    protect_blank_node_scaffolding: bool = False
    allow_blank_seed_entities: bool = False


@dataclass
class RandomFactConfig:
    enabled: bool = True


@dataclass
class EntityConfig:
    enabled: bool = True
    mode: str = "incident"
    entity_sampling: str = "uniform"


@dataclass
class PredicateConfig:
    enabled: bool = True
    mode: str = "whole_predicates"
    predicate_sampling: str = "uniform"


@dataclass
class LongTailConfig:
    enabled: bool = True
    basis: str = "predicate_frequency"
    alpha: float = 1.0


@dataclass
class SourceConfig:
    """Source-centric family. Disabled by default until provenance is supplied."""

    enabled: bool = False
    provenance_file: Path | None = None
    multi_source_policy: str = "remove_if_all_selected"
    named_graph_as_source: bool = True


@dataclass
class SubgraphConfig:
    enabled: bool = True
    traversal: str = "undirected"
    max_hops: int = 2
    include_literal_properties: bool = True
    seed_entity_sampling: str = "uniform"
    max_cluster_fraction: float | None = None


@dataclass
class FamiliesConfig:
    random_fact: RandomFactConfig = field(default_factory=RandomFactConfig)
    entity: EntityConfig = field(default_factory=EntityConfig)
    predicate: PredicateConfig = field(default_factory=PredicateConfig)
    long_tail: LongTailConfig = field(default_factory=LongTailConfig)
    source: SourceConfig = field(default_factory=SourceConfig)
    subgraph: SubgraphConfig = field(default_factory=SubgraphConfig)


@dataclass
class CorruptionConfig:
    input: InputConfig = field(default_factory=InputConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    benchmark: BenchmarkConfig = field(default_factory=BenchmarkConfig)
    eligibility: EligibilityConfig = field(default_factory=EligibilityConfig)
    families: FamiliesConfig = field(default_factory=FamiliesConfig)

    def enabled_family_names(self) -> list[str]:
        mapping = {
            "random_fact": self.families.random_fact.enabled,
            "entity": self.families.entity.enabled,
            "predicate": self.families.predicate.enabled,
            "long_tail": self.families.long_tail.enabled,
            "source": self.families.source.enabled,
            "subgraph": self.families.subgraph.enabled,
        }
        return [name for name, enabled in mapping.items() if enabled]

    def family_config_dict(self, family: str) -> dict[str, Any]:
        section = getattr(self.families, family)
        return _to_plain(asdict(section))

    def to_plain(self) -> dict[str, Any]:
        return _to_plain(asdict(self))


def _to_plain(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(item) for item in value]
    return value


PATH_KEYS = {
    "root_reference",
    "directory",
    "provenance_file",
}


def _merge_dataclass(cls, data: Mapping[str, Any] | None, *, base: Path | None):
    instance = cls()
    if not data:
        return instance
    valid = {item.name for item in fields(cls)}
    for key, value in data.items():
        if key not in valid:
            raise ValueError(f"Unknown config key for {cls.__name__}: {key}")
        current = getattr(instance, key)
        if hasattr(current, "__dataclass_fields__") and isinstance(value, Mapping):
            setattr(instance, key, _merge_dataclass(type(current), value, base=base))
        else:
            setattr(instance, key, _coerce(current, value, base=base, key=key))
    return instance


def _coerce(current: Any, value: Any, *, base: Path | None, key: str = "") -> Any:
    if isinstance(current, Path) or key in PATH_KEYS:
        return _resolve_path(value, base)
    if isinstance(current, bool):
        return bool(value)
    if isinstance(current, int) and not isinstance(current, bool):
        return int(value)
    if isinstance(current, float):
        return float(value) if value is not None else None
    if isinstance(current, list):
        return list(value)
    if current is None and isinstance(value, (int, float)):
        return float(value)
    return value


def _resolve_path(value: Any, base: Path | None) -> Path | None:
    if value is None or value == "":
        return None
    path = Path(str(value)).expanduser()
    if path.is_absolute() or base is None:
        return path
    return (base / path).resolve()


def load_config(path: str | Path | None = None, data: Mapping[str, Any] | None = None) -> CorruptionConfig:
    """Load YAML config. Relative paths resolve against the config file directory."""
    mapping: dict[str, Any] = {}
    base: Path | None = None
    if path is not None:
        config_path = Path(path)
        base = config_path.parent.resolve()
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, Mapping):
            raise ValueError(f"Config root must be a mapping: {config_path}")
        mapping.update(loaded)
    if data:
        mapping.update(data)
    return _merge_dataclass(CorruptionConfig, mapping, base=base)


def dump_config_yaml(config: CorruptionConfig) -> str:
    return yaml.safe_dump(config.to_plain(), sort_keys=False, allow_unicode=True)
