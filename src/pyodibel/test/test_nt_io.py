"""Tests for NT file and Spark part-directory loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from pyodibel.operations.rdf.nt_io import load_nt, nt_input_paths


T1 = '<http://ex/s1> <http://ex/p> <http://ex/o1> .\n'
T2 = '<http://ex/s2> <http://ex/p> <http://ex/o2> .\n'


def test_load_nt_file(tmp_path: Path):
    path = tmp_path / "graph.nt"
    path.write_text(T1 + T2, encoding="utf-8")
    triples = load_nt(path)
    assert len(triples) == 2
    assert triples[0][0] == "<http://ex/s1>"


def test_load_nt_spark_directory(tmp_path: Path):
    spark_dir = tmp_path / "property_filter"
    spark_dir.mkdir()
    (spark_dir / "_SUCCESS").write_text("", encoding="utf-8")
    (spark_dir / ".part-00000.crc").write_text("crc", encoding="utf-8")
    (spark_dir / "part-00001-aaaa-c000.txt").write_text(T2, encoding="utf-8")
    (spark_dir / "part-00000-aaaa-c000.txt").write_text(T1, encoding="utf-8")

    parts = nt_input_paths(spark_dir)
    assert [p.name for p in parts] == [
        "part-00000-aaaa-c000.txt",
        "part-00001-aaaa-c000.txt",
    ]

    triples = load_nt(spark_dir)
    subjects = [subject for subject, _, _, _ in triples]
    assert subjects == ["<http://ex/s1>", "<http://ex/s2>"]


def test_load_nt_rdflib_spark_directory(tmp_path: Path):
    spark_dir = tmp_path / "property_filter"
    spark_dir.mkdir()
    (spark_dir / "_SUCCESS").write_text("", encoding="utf-8")
    (spark_dir / "part-00000-aaaa-c000.txt").write_text(T1, encoding="utf-8")
    (spark_dir / "part-00001-aaaa-c000.txt").write_text(T2, encoding="utf-8")

    triples = load_nt(spark_dir, parser="rdflib")
    subjects = {subject for subject, _, _, _ in triples}
    assert subjects == {"<http://ex/s1>", "<http://ex/s2>"}


def test_nt_input_paths_empty_dir(tmp_path: Path):
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "_SUCCESS").write_text("", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="no NT part files"):
        nt_input_paths(empty)
