"""Dataset card model and builders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from pyodibel.datasets.kgi_provenance.config import GenerationConfig, config_hash
from pyodibel.datasets.kgi_provenance.identity import build_dataset_id, display_projection


CARD_SCHEMA_VERSION = "1.0.0"


@dataclass
class MeasuredOutcomes:
    """Post-generation characterization (not part of identity hash)."""

    root_entity_counts: dict[str, int] = field(default_factory=dict)
    entity_counts: dict[str, int] = field(default_factory=dict)
    triple_counts: dict[str, int] = field(default_factory=dict)
    seed_overlaps: dict[str, float] = field(default_factory=dict)
    subgraph_overlaps: dict[str, Any] = field(default_factory=dict)
    source_availability: dict[str, Any] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    ontology_fitness: dict[str, Any] = field(default_factory=dict)
    quality_gate: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "root_entity_counts": self.root_entity_counts,
            "entity_counts": self.entity_counts,
            "triple_counts": self.triple_counts,
            "seed_overlaps": self.seed_overlaps,
            "subgraph_overlaps": self.subgraph_overlaps,
            "source_availability": self.source_availability,
            "coverage": self.coverage,
            "ontology_fitness": self.ontology_fitness,
            "quality_gate": self.quality_gate,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> MeasuredOutcomes:
        data = data or {}
        return cls(
            root_entity_counts=dict(data.get("root_entity_counts") or {}),
            entity_counts=dict(data.get("entity_counts") or {}),
            triple_counts=dict(data.get("triple_counts") or {}),
            seed_overlaps=dict(data.get("seed_overlaps") or {}),
            subgraph_overlaps=dict(data.get("subgraph_overlaps") or {}),
            source_availability=dict(data.get("source_availability") or {}),
            coverage=dict(data.get("coverage") or {}),
            ontology_fitness=dict(data.get("ontology_fitness") or {}),
            quality_gate=dict(data.get("quality_gate") or {}),
            extra=dict(data.get("extra") or {}),
        )


@dataclass
class DatasetCard:
    schema_version: str
    dataset_id: str
    display_name: str
    created_at: str
    status: str
    motivation: str | None
    identity: dict[str, Any]
    inputs: dict[str, Any]
    recipe: dict[str, Any]
    artifacts: dict[str, Any]
    measured: MeasuredOutcomes
    intended_use: str
    limitations: list[str]
    lineage: dict[str, Any]
    software: dict[str, Any] = field(default_factory=dict)
    legacy: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "display_name": self.display_name,
            "created_at": self.created_at,
            "status": self.status,
            "motivation": self.motivation,
            "identity": self.identity,
            "inputs": self.inputs,
            "recipe": self.recipe,
            "artifacts": self.artifacts,
            "measured": self.measured.to_dict(),
            "intended_use": self.intended_use,
            "limitations": list(self.limitations),
            "lineage": self.lineage,
            "software": self.software,
            "legacy": self.legacy,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DatasetCard:
        return cls(
            schema_version=str(data.get("schema_version", CARD_SCHEMA_VERSION)),
            dataset_id=str(data["dataset_id"]),
            display_name=str(data.get("display_name") or data["dataset_id"]),
            created_at=str(data.get("created_at") or ""),
            status=str(data.get("status") or "draft"),
            motivation=data.get("motivation"),
            identity=dict(data.get("identity") or {}),
            inputs=dict(data.get("inputs") or {}),
            recipe=dict(data.get("recipe") or {}),
            artifacts=dict(data.get("artifacts") or {}),
            measured=MeasuredOutcomes.from_dict(data.get("measured")),
            intended_use=str(
                data.get("intended_use")
                or "SSP/MSP KG integration pipeline evaluation (KGI-Bench)"
            ),
            limitations=list(data.get("limitations") or []),
            lineage=dict(data.get("lineage") or {}),
            software=dict(data.get("software") or {}),
            legacy=bool(data.get("legacy", False)),
        )


def _default_limitations(config: GenerationConfig) -> list[str]:
    limitations: list[str] = []
    if not config.recipe.formats.json:
        limitations.append("JSON sources not included in this build")
    if not config.recipe.formats.text:
        limitations.append("Text sources not included in this build")
    if not config.recipe.gold_matches:
        limitations.append("verified_matches gold not emitted")
    if not config.recipe.gold_links:
        limitations.append("verified_links gold not emitted")
    limitations.append(
        "Text2KGBench ontologies may mistype datatype properties as object properties"
    )
    return limitations


def build_dataset_card(
    config: GenerationConfig,
    *,
    dataset_id: str | None = None,
    measured: MeasuredOutcomes | Mapping[str, Any] | None = None,
    artifacts: Mapping[str, Any] | None = None,
    motivation: str | None = None,
    status: str = "draft",
    limitations: list[str] | None = None,
    lineage: Mapping[str, Any] | None = None,
    created_at: str | None = None,
    legacy: bool = False,
) -> DatasetCard:
    """Assemble a dataset card from a generation config and optional measurements."""
    resolved_id = dataset_id or build_dataset_id(config)
    digest = config_hash(config)
    measured_obj = (
        measured
        if isinstance(measured, MeasuredOutcomes)
        else MeasuredOutcomes.from_dict(measured)
    )
    lineage_dict = dict(lineage or {})
    return DatasetCard(
        schema_version=CARD_SCHEMA_VERSION,
        dataset_id=resolved_id,
        display_name=display_projection(config),
        created_at=created_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        status=status,
        motivation=motivation,
        identity={
            "config_hash": digest,
            "config_path": "generation_config.yaml",
            "card_path": "dataset_card.json",
        },
        inputs=config.inputs.to_dict(),
        recipe=config.recipe.to_dict(),
        artifacts=dict(artifacts or {}),
        measured=measured_obj,
        intended_use=(
            "SSP/MSP knowledge graph integration pipeline evaluation (KGI-Bench); "
            "not a general KG QA benchmark"
        ),
        limitations=limitations if limitations is not None else _default_limitations(config),
        lineage={
            "parent_subgraph_id": config.parent_subgraph_id,
            "parent_dataset_id": lineage_dict.get("parent_dataset_id"),
            "related_experiment_ids": list(
                lineage_dict.get("related_experiment_ids") or []
            ),
            **{
                key: value
                for key, value in lineage_dict.items()
                if key
                not in {
                    "parent_subgraph_id",
                    "parent_dataset_id",
                    "related_experiment_ids",
                }
            },
        },
        software=config.software.to_dict(),
        legacy=legacy,
    )
