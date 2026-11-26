"""Tests for .odibel change detection and version history."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from pyodibel.cli.main import cli
from pyodibel.datasets.manifest import read_checksums, verify, write_checksums
from pyodibel.datasets.versions import VersionHistory, commit, status

MANIFEST = """\
odibel: "0.1"
id: example-mini
title: Example minidump
status: draft
kind: rdf-ntriples
license: CC0-1.0
"""


def _age(root: Path, seconds: int = 100) -> None:
    """Move payload mtimes into the past so the stat cache trusts them."""
    past = time.time() - seconds
    for path in root.rglob("*"):
        if path.is_file() and ".odibel" not in path.parts:
            os.utime(path, (past, past))


def _dataset(tmp_path: Path) -> Path:
    root = tmp_path / "example-mini"
    (root / ".odibel").mkdir(parents=True)
    (root / ".odibel" / "dataset.yaml").write_text(MANIFEST, encoding="utf-8")
    (root / "data.nt").write_text("<http://ex/s> <http://ex/p> <http://ex/o> .\n", encoding="utf-8")
    (root / "seeds").mkdir()
    (root / "seeds" / "seeds.txt").write_text("<http://ex/s>\n", encoding="utf-8")
    _age(root)
    return root


def test_status_uses_stat_cache(tmp_path: Path):
    root = _dataset(tmp_path)
    write_checksums(root)

    first = status(root)
    assert not first.changes
    assert first.hashed == 2
    assert status(root).hashed == 0

    # Same size and mtime: the cache is trusted, only --checksum sees the edit.
    st = os.stat(root / "data.nt")
    (root / "data.nt").write_text("<http://ex/s> <http://ex/p> <http://ex/X> .\n", encoding="utf-8")
    os.utime(root / "data.nt", ns=(st.st_atime_ns, st.st_mtime_ns))
    assert not status(root).changes
    assert status(root, checksum=True).changes.modified == ["data.nt"]


def test_status_rehashes_recent_and_changed_files(tmp_path: Path):
    root = _dataset(tmp_path)
    write_checksums(root)
    status(root)

    (root / "data.nt").write_text("<http://ex/s> <http://ex/p> <http://ex/X> .\n", encoding="utf-8")
    (root / "seeds" / "seeds.txt").unlink()
    (root / "extra.txt").write_text("new\n", encoding="utf-8")

    report = status(root)
    assert report.hashed == 2
    assert report.changes.modified == ["data.nt"]
    assert report.changes.removed == ["seeds/seeds.txt"]
    assert report.changes.added == ["extra.txt"]


def test_status_without_checksum_file_lists_everything_as_added(tmp_path: Path):
    root = _dataset(tmp_path)
    report = status(root)
    assert report.version is None
    assert report.changes.added == ["data.nt", "seeds/seeds.txt"]


def test_commit_records_versions(tmp_path: Path):
    root = _dataset(tmp_path)

    v1 = commit(root, "initial", labels=("stable",))
    assert v1["version"] == 1 and v1["parent"] is None
    assert v1["changes"] == {"added": 2, "removed": 0, "modified": 0}
    assert verify(root).ok
    assert read_checksums(root, root / ".odibel" / "versions" / "1.sha256") == read_checksums(root)

    with pytest.raises(ValueError, match="no changes since version 1"):
        commit(root, "nothing")

    (root / "data.nt").write_text("changed\n", encoding="utf-8")
    v2 = commit(root, "small change")
    assert v2["version"] == 2 and v2["parent"] == 1
    assert v2["changes"] == {"added": 0, "removed": 0, "modified": 1}
    assert verify(root).ok

    history = VersionHistory.load(root)
    assert [v["version"] for v in history.versions] == [1, 2]
    assert history.labels == {"stable": 1}
    assert history.file_list(1)["data.nt"] != history.file_list(2)["data.nt"]
    assert not status(root).changes


def test_commit_rejects_reserved_labels(tmp_path: Path):
    root = _dataset(tmp_path)
    for label in ("latest", "3"):
        with pytest.raises(ValueError, match="invalid label"):
            commit(root, "x", labels=(label,))
    assert VersionHistory.load(root).versions == []


def test_cli_status_commit_log(tmp_path: Path):
    root = _dataset(tmp_path)
    runner = CliRunner()

    result = runner.invoke(cli, ["data", "status", str(root)])
    assert result.exit_code == 0
    assert "2 changes against files.sha256" in result.output

    assert runner.invoke(cli, ["data", "commit", str(root), "-m", "initial", "--label", "stable"]).exit_code == 0
    result = runner.invoke(cli, ["data", "commit", str(root), "-m", "again"])
    assert result.exit_code == 1
    assert "no changes since version 1" in result.output

    result = runner.invoke(cli, ["data", "status", str(root)])
    assert "clean against version 1" in result.output

    result = runner.invoke(cli, ["data", "log", str(root)])
    assert result.exit_code == 0
    assert "v1" in result.output and "[latest, stable]" in result.output
    assert "initial" in result.output
