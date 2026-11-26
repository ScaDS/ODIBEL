"""
Change detection and version history for datasets with a ``.odibel`` directory.

Layout (additions to ``manifest``)::

    <dataset>/.odibel/
      versions.yaml              # version entries and labels
      versions/<n>.sha256        # file list of version <n>, ``sha256sum`` format
      cache/stat.json            # local only: size, mtime, inode and sha256 per file

``status`` re-hashes a file only when its size, mtime or inode differ from the
cache, or when its mtime is too close to the time the cache was written to be
trusted. ``checksum=True`` re-hashes every file.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from pyodibel.datasets.manifest import (
    CHECKSUM_FILE,
    META_DIR,
    SCHEMA_VERSION,
    payload_files,
    read_checksums,
    sha256_file,
    write_checksum_file,
)

VERSIONS_FILE = "versions.yaml"
VERSIONS_DIR = "versions"
CACHE_FILE = "cache/stat.json"
RESERVED_LABELS = ("latest",)

# Coarse filesystem timestamps (FAT: 2 s) can hide a same-size rewrite shortly
# after the cache was written; entries modified within this window are re-hashed.
RACY_WINDOW_NS = 2_000_000_000


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _load_cache(root: Path) -> dict[str, Any]:
    try:
        cache = json.loads((root / META_DIR / CACHE_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"written_ns": 0, "files": {}}
    if not isinstance(cache, dict) or not isinstance(cache.get("files"), dict):
        return {"written_ns": 0, "files": {}}
    return cache


def _save_cache(root: Path, files: dict[str, dict[str, int | str]]) -> None:
    cache = {"written_ns": time.time_ns(), "files": files}
    _write_atomic(root / META_DIR / CACHE_FILE, json.dumps(cache, indent=0, sort_keys=True))


def current_checksums(root: str | Path, *, checksum: bool = False) -> tuple[dict[str, str], dict[str, int], int]:
    """
    sha256 per payload file, using the stat cache unless ``checksum``.

    Returns ``(checksums, sizes, hashed)`` where ``hashed`` counts files read in full.
    """
    root = Path(root)
    cache = _load_cache(root)
    trusted_before = int(cache.get("written_ns", 0)) - RACY_WINDOW_NS
    cached = cache["files"]

    checksums: dict[str, str] = {}
    sizes: dict[str, int] = {}
    entries: dict[str, dict[str, int | str]] = {}
    hashed = 0
    for name in payload_files(root):
        st = os.stat(root / name)
        entry = cached.get(name)
        if (
            not checksum
            and isinstance(entry, dict)
            and entry.get("size") == st.st_size
            and entry.get("mtime_ns") == st.st_mtime_ns
            and entry.get("ino") == st.st_ino
            and st.st_mtime_ns < trusted_before
        ):
            digest = str(entry["sha256"])
        else:
            digest = sha256_file(root / name)
            hashed += 1
        checksums[name] = digest
        sizes[name] = st.st_size
        entries[name] = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "ino": st.st_ino, "sha256": digest}
    _save_cache(root, entries)
    return checksums, sizes, hashed


@dataclass
class Changes:
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.added or self.removed or self.modified)

    def counts(self) -> dict[str, int]:
        return {"added": len(self.added), "removed": len(self.removed), "modified": len(self.modified)}


def compare(old: dict[str, str], new: dict[str, str]) -> Changes:
    return Changes(
        added=sorted(set(new) - set(old)),
        removed=sorted(set(old) - set(new)),
        modified=sorted(name for name in set(old) & set(new) if old[name] != new[name]),
    )


@dataclass
class VersionHistory:
    root: Path
    versions: list[dict[str, Any]] = field(default_factory=list)
    labels: dict[str, int] = field(default_factory=dict)

    @property
    def latest(self) -> dict[str, Any] | None:
        return self.versions[-1] if self.versions else None

    def file_list(self, version: int) -> dict[str, str]:
        return read_checksums(self.root, self.root / META_DIR / VERSIONS_DIR / f"{version}.sha256")

    def write(self) -> None:
        data = {"odibel": SCHEMA_VERSION, "labels": self.labels, "versions": self.versions}
        text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=100)
        _write_atomic(self.root / META_DIR / VERSIONS_FILE, text)

    @classmethod
    def load(cls, root: str | Path) -> VersionHistory:
        root = Path(root)
        path = root / META_DIR / VERSIONS_FILE
        if not path.exists():
            return cls(root=root)
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a mapping")
        return cls(root=root, versions=list(data.get("versions") or []), labels=dict(data.get("labels") or {}))


@dataclass
class StatusReport:
    version: int | None
    changes: Changes
    files: int
    hashed: int
    checksums: dict[str, str] = field(repr=False, default_factory=dict)
    sizes: dict[str, int] = field(repr=False, default_factory=dict)


def baseline(root: str | Path, history: VersionHistory | None = None) -> tuple[int | None, dict[str, str]]:
    """File list to compare against: the latest version, else ``files.sha256``, else empty."""
    root = Path(root)
    history = history or VersionHistory.load(root)
    if history.latest:
        version = int(history.latest["version"])
        return version, history.file_list(version)
    if (root / META_DIR / CHECKSUM_FILE).exists():
        return None, read_checksums(root)
    return None, {}


def status(root: str | Path, *, checksum: bool = False) -> StatusReport:
    root = Path(root)
    version, old = baseline(root)
    checksums, sizes, hashed = current_checksums(root, checksum=checksum)
    return StatusReport(
        version=version,
        changes=compare(old, checksums),
        files=len(checksums),
        hashed=hashed,
        checksums=checksums,
        sizes=sizes,
    )


def commit(
    root: str | Path,
    message: str,
    *,
    labels: tuple[str, ...] = (),
    checksum: bool = False,
    allow_empty: bool = False,
) -> dict[str, Any]:
    """
    Record the current payload as a new version.

    Writes ``versions/<n>.sha256``, updates ``files.sha256`` and appends an entry
    to ``versions.yaml``. The first commit always creates version 1.
    """
    root = Path(root)
    for label in labels:
        if label in RESERVED_LABELS or label.isdigit():
            raise ValueError(f"invalid label: {label}")
    history = VersionHistory.load(root)
    report = status(root, checksum=checksum)
    if history.latest and not report.changes and not allow_empty:
        raise ValueError(f"no changes since version {report.version}")

    number = int(history.latest["version"]) + 1 if history.latest else 1
    parent_files = history.file_list(number - 1) if history.latest else {}
    list_path = root / META_DIR / VERSIONS_DIR / f"{number}.sha256"
    write_checksum_file(list_path, report.checksums)
    write_checksum_file(root / META_DIR / CHECKSUM_FILE, report.checksums)

    entry = {
        "version": number,
        "parent": number - 1 if history.latest else None,
        "created": datetime.now().astimezone().isoformat(timespec="seconds"),
        "message": message,
        "sha256": sha256_file(list_path),
        "files": report.files,
        "bytes": sum(report.sizes.values()),
        "changes": compare(parent_files, report.checksums).counts(),
    }
    history.versions.append(entry)
    for label in labels:
        history.labels[label] = number
    history.write()
    return entry
