"""Manifest rows, CSV/JSONL writers, and path helpers."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

MANIFEST_FIELDS = [
    "graph_id",
    "family",
    "seed",
    "root_graph_path",
    "root_triple_count",
    "eligible_triple_count",
    "protected_triple_count",
    "target_removed_fraction",
    "target_removed_count",
    "realized_removed_fraction",
    "realized_removed_count",
    "retained_eval_count",
    "reference_coverage",
    "output_graph_path",
    "removed_triples_path",
    "metadata_path",
    "strategy_version",
    "config_hash",
    "root_graph_hash",
    "created_at",
]


def fraction_folder(fraction: float) -> str:
    return f"remove_{int(round(fraction * 100)):03d}"


def graph_id_for(family: str, fraction: float, seed: int) -> str:
    return f"{family}_{fraction_folder(fraction)}_seed_{int(seed):03d}"


def output_stem(family: str, fraction: float, seed: int, directory: Path) -> Path:
    return directory / family / fraction_folder(fraction) / f"seed_{int(seed):03d}"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def read_manifest(path: Path) -> list[dict[str, Any]]:
    path = Path(path)
    if path.suffix.lower() == ".jsonl":
        rows = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def iter_manifest_rows(path: Path) -> Iterator[dict[str, Any]]:
    yield from read_manifest(path)
