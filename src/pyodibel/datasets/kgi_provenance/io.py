"""Load/save generation configs and dataset cards."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from pyodibel.datasets.kgi_provenance.card import DatasetCard
from pyodibel.datasets.kgi_provenance.config import GenerationConfig


def save_generation_config(config: GenerationConfig, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            config.to_dict(),
            handle,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
        )
    return out


def load_generation_config(path: str | Path) -> GenerationConfig:
    with Path(path).open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"generation config must be a mapping: {path}")
    return GenerationConfig.from_dict(data)


def save_dataset_card(card: DatasetCard, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        json.dump(card.to_dict(), handle, indent=2, ensure_ascii=True)
        handle.write("\n")
    return out


def load_dataset_card(path: str | Path) -> DatasetCard:
    with Path(path).open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"dataset card must be a JSON object: {path}")
    return DatasetCard.from_dict(data)


def save_json(data: Any, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=True)
        handle.write("\n")
    return out
