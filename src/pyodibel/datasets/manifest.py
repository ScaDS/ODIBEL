"""
Dataset metadata kept in a ``.odibel`` directory next to the data.

Layout::

    <dataset>/
      <payload files>              # not in Git; local copy or symlink
      .odibel/
        dataset.yaml               # identity, lineage, recipe, contract
        files.sha256               # ``sha256sum`` format, paths relative to <dataset>
        stats.json                 # optional measured statistics

Only ``.odibel/`` is versioned. Payloads are identified by their checksums.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

META_DIR = ".odibel"
MANIFEST_FILE = "dataset.yaml"
CHECKSUM_FILE = "files.sha256"
STATS_FILE = "stats.json"

SCHEMA_VERSION = "0.1"
REQUIRED_KEYS = ("odibel", "id", "title", "status", "kind", "license")
STATUSES = ("draft", "published", "deprecated")

_CHUNK = 1 << 20


@dataclass
class DatasetManifest:
    root: Path
    data: dict[str, Any]

    @property
    def id(self) -> str:
        return str(self.data["id"])

    @property
    def meta_dir(self) -> Path:
        return self.root / META_DIR

    def write(self) -> Path:
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        path = self.meta_dir / MANIFEST_FILE
        with path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(self.data, handle, sort_keys=False, allow_unicode=True, width=100)
        return path

    @classmethod
    def load(cls, root: str | Path) -> DatasetManifest:
        root = Path(root)
        path = root / META_DIR / MANIFEST_FILE
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a mapping")
        return cls(root=root, data=data)

    def validate(self) -> list[str]:
        problems = [f"missing key: {key}" for key in REQUIRED_KEYS if key not in self.data]
        status = self.data.get("status")
        if status is not None and status not in STATUSES:
            problems.append(f"invalid status: {status} (expected one of {', '.join(STATUSES)})")
        for i, parent in enumerate(self.data.get("derived_from") or []):
            if not isinstance(parent, dict) or not parent.get("id") or not parent.get("sha256"):
                problems.append(f"derived_from[{i}]: needs id and sha256")
        return problems


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def payload_files(root: str | Path) -> list[str]:
    """Relative paths of all payload files, following symlinks, excluding ``.odibel``."""
    root = Path(root)
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        rel_dir = Path(dirpath).relative_to(root)
        if rel_dir == Path("."):
            dirnames[:] = [d for d in dirnames if d != META_DIR]
        for name in filenames:
            found.append((rel_dir / name).as_posix())
    return sorted(found)


def read_checksums(root: str | Path) -> dict[str, str]:
    path = Path(root) / META_DIR / CHECKSUM_FILE
    checksums: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, name = line.split(maxsplit=1)
        checksums[name.lstrip("*")] = digest
    return checksums


def dataset_digest(root: str | Path) -> str:
    """sha256 of ``files.sha256``; identifies a multi-file payload in ``derived_from``."""
    return sha256_file(Path(root) / META_DIR / CHECKSUM_FILE)


def write_checksums(root: str | Path) -> dict[str, str]:
    root = Path(root)
    checksums = {name: sha256_file(root / name) for name in payload_files(root)}
    meta = root / META_DIR
    meta.mkdir(exist_ok=True)
    lines = [f"{digest}  {name}\n" for name, digest in checksums.items()]
    (meta / CHECKSUM_FILE).write_text("".join(lines), encoding="utf-8")
    return checksums


@dataclass
class VerifyReport:
    dataset_id: str | None
    problems: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    mismatched: list[str] = field(default_factory=list)
    unlisted: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.problems or self.missing or self.mismatched or self.unlisted)


def verify(root: str | Path, *, metadata_only: bool = False) -> VerifyReport:
    """
    Check the manifest and, unless ``metadata_only``, the payload checksums.

    ``metadata_only`` is for checkouts without the payload: it validates
    ``dataset.yaml`` and that ``files.sha256`` is readable.
    """
    root = Path(root)
    try:
        manifest = DatasetManifest.load(root)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return VerifyReport(dataset_id=None, problems=[f"manifest: {exc}"])

    report = VerifyReport(dataset_id=manifest.data.get("id"), problems=manifest.validate())
    try:
        expected = read_checksums(root)
    except (OSError, ValueError) as exc:
        report.problems.append(f"checksums: {exc}")
        return report
    if metadata_only:
        return report

    present = set(payload_files(root))
    for name, digest in expected.items():
        if name not in present:
            report.missing.append(name)
        elif sha256_file(root / name) != digest:
            report.mismatched.append(name)
    report.unlisted = sorted(present - set(expected))
    return report
