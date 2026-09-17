"""Dataset registry index (dataset_id → path + card summary)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pyodibel.datasets.kgi_provenance.card import DatasetCard


REGISTRY_SCHEMA_VERSION = "1.0.0"


@dataclass
class RegistryEntry:
    dataset_id: str
    path: str
    display_name: str
    domain: str
    status: str
    config_hash: str
    created_at: str
    legacy: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "path": self.path,
            "display_name": self.display_name,
            "domain": self.domain,
            "status": self.status,
            "config_hash": self.config_hash,
            "created_at": self.created_at,
            "legacy": self.legacy,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RegistryEntry:
        return cls(
            dataset_id=str(data["dataset_id"]),
            path=str(data["path"]),
            display_name=str(data.get("display_name") or data["dataset_id"]),
            domain=str(data.get("domain") or ""),
            status=str(data.get("status") or "draft"),
            config_hash=str(data.get("config_hash") or ""),
            created_at=str(data.get("created_at") or ""),
            legacy=bool(data.get("legacy", False)),
        )

    @classmethod
    def from_card(cls, card: DatasetCard, *, path: str | Path) -> RegistryEntry:
        domain = str((card.inputs or {}).get("domain") or "")
        return cls(
            dataset_id=card.dataset_id,
            path=str(path),
            display_name=card.display_name,
            domain=domain,
            status=card.status,
            config_hash=str((card.identity or {}).get("config_hash") or ""),
            created_at=card.created_at,
            legacy=card.legacy,
        )


def load_registry(path: str | Path) -> dict[str, Any]:
    registry_path = Path(path)
    if not registry_path.exists():
        return {"schema_version": REGISTRY_SCHEMA_VERSION, "datasets": {}}
    with registry_path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"registry must be a JSON object: {path}")
    data.setdefault("schema_version", REGISTRY_SCHEMA_VERSION)
    data.setdefault("datasets", {})
    return data


def save_registry(registry: Mapping[str, Any], path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": registry.get("schema_version", REGISTRY_SCHEMA_VERSION),
        "datasets": dict(registry.get("datasets") or {}),
    }
    with out.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, sort_keys=True)
        handle.write("\n")
    return out


def register_dataset(
    registry_path: str | Path,
    card: DatasetCard,
    *,
    dataset_path: str | Path,
) -> dict[str, Any]:
    """Upsert a dataset into registry.json and return the updated registry."""
    registry = load_registry(registry_path)
    entry = RegistryEntry.from_card(card, path=dataset_path)
    registry["datasets"][card.dataset_id] = entry.to_dict()
    save_registry(registry, registry_path)
    return registry


def list_datasets_by_domain(
    registry: Mapping[str, Any],
    domain: str,
) -> list[RegistryEntry]:
    entries = []
    for item in (registry.get("datasets") or {}).values():
        entry = RegistryEntry.from_dict(item)
        if entry.domain == domain:
            entries.append(entry)
    return sorted(entries, key=lambda value: value.dataset_id)
