"""Tests for .odibel dataset metadata."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from pyodibel.cli.main import cli
from pyodibel.datasets.manifest import (
    DatasetManifest,
    payload_files,
    read_checksums,
    verify,
    write_checksums,
)

MANIFEST = """\
odibel: "0.1"
id: example-mini
title: Example minidump
status: draft
kind: rdf-ntriples
license: CC0-1.0
derived_from:
  - id: example-full
    sha256: abc123
"""


def _dataset(tmp_path: Path) -> Path:
    root = tmp_path / "example-mini"
    (root / ".odibel").mkdir(parents=True)
    (root / ".odibel" / "dataset.yaml").write_text(MANIFEST, encoding="utf-8")
    (root / "data.nt").write_text("<http://ex/s> <http://ex/p> <http://ex/o> .\n", encoding="utf-8")
    (root / "seeds").mkdir()
    (root / "seeds" / "seeds.txt").write_text("<http://ex/s>\n", encoding="utf-8")
    return root


def test_payload_files_skip_meta_dir_and_follow_symlinks(tmp_path: Path):
    root = _dataset(tmp_path)
    external = tmp_path / "external.nt"
    external.write_text("x\n", encoding="utf-8")
    (root / "full.nt").symlink_to(external)
    assert payload_files(root) == ["data.nt", "full.nt", "seeds/seeds.txt"]


def test_write_and_verify_checksums(tmp_path: Path):
    root = _dataset(tmp_path)
    checksums = write_checksums(root)
    assert read_checksums(root) == checksums
    assert verify(root).ok


def test_verify_reports_changed_missing_and_unlisted_files(tmp_path: Path):
    root = _dataset(tmp_path)
    write_checksums(root)
    (root / "data.nt").write_text("changed\n", encoding="utf-8")
    (root / "seeds" / "seeds.txt").unlink()
    (root / "extra.txt").write_text("new\n", encoding="utf-8")

    report = verify(root)
    assert not report.ok
    assert report.mismatched == ["data.nt"]
    assert report.missing == ["seeds/seeds.txt"]
    assert report.unlisted == ["extra.txt"]


def test_verify_metadata_only_ignores_absent_payload(tmp_path: Path):
    root = _dataset(tmp_path)
    write_checksums(root)
    (root / "data.nt").unlink()
    assert verify(root, metadata_only=True).ok
    assert not verify(root).ok


def test_manifest_validation(tmp_path: Path):
    root = _dataset(tmp_path)
    (root / ".odibel" / "dataset.yaml").write_text(
        "odibel: '0.1'\nid: x\nstatus: final\nderived_from: [{id: y}]\n", encoding="utf-8"
    )
    problems = DatasetManifest.load(root).validate()
    assert "missing key: title" in problems
    assert any(p.startswith("invalid status: final") for p in problems)
    assert "derived_from[0]: needs id and sha256" in problems


def test_cli_hash_and_verify(tmp_path: Path):
    root = _dataset(tmp_path)
    runner = CliRunner()
    assert runner.invoke(cli, ["data", "verify", str(root)]).exit_code == 1
    assert runner.invoke(cli, ["data", "hash", str(root)]).exit_code == 0
    result = runner.invoke(cli, ["data", "verify", str(root)])
    assert result.exit_code == 0
    assert "ok      example-mini" in result.output
