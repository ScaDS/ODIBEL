from __future__ import annotations

from kg_corrupt.config import CorruptionConfig
from kg_corrupt.generator import generate_benchmark
from kg_corrupt.io import load_graph
from kg_corrupt.manifest import read_manifest
from support import people_graph


def test_manifest_matches_measured_graphs(tmp_path):
    graph = people_graph()
    root = tmp_path / "root.ttl"
    graph.serialize(root.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = root
    config.output.directory = tmp_path / "out"
    config.benchmark.seeds = [1, 2]
    config.benchmark.removal_fractions = [0.25, 0.5]
    config.families.entity.enabled = False
    config.families.predicate.enabled = False
    config.families.long_tail.enabled = False
    config.families.subgraph.enabled = False
    config.families.source.enabled = False
    rows = generate_benchmark(config)
    assert (tmp_path / "out" / "benchmark_manifest.csv").is_file()
    assert (tmp_path / "out" / "benchmark_manifest.jsonl").is_file()
    assert (tmp_path / "out" / "root_stats.json").is_file()
    assert (tmp_path / "out" / "reproducibility.json").is_file()
    assert len(rows) == 4  # 1 family × 2 fractions × 2 seeds

    from kg_corrupt.generator import prepare_root

    prepared = prepare_root(config)
    for row in rows:
        retained = load_graph(row["output_graph_path"])
        removed = load_graph(row["removed_triples_path"])
        measured_removed = set(prepared.eligibility.eligible) - set(retained)
        assert measured_removed == set(removed)
        assert int(row["realized_removed_count"]) == len(measured_removed)
        coverage = 1.0 - (
            len(measured_removed) / prepared.eligibility.eligible_count
        )
        assert abs(float(row["reference_coverage"]) - coverage) < 1e-12
        assert row["graph_id"]
        assert (tmp_path / row["metadata_path"]).is_file()


def test_csv_and_jsonl_manifests_agree(tmp_path):
    graph = people_graph()
    root = tmp_path / "root.ttl"
    graph.serialize(root.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = root
    config.output.directory = tmp_path / "out"
    config.benchmark.seeds = [1]
    config.benchmark.removal_fractions = [0.5]
    config.families.entity.enabled = False
    config.families.predicate.enabled = False
    config.families.long_tail.enabled = False
    config.families.subgraph.enabled = False
    generate_benchmark(config)
    csv_rows = read_manifest(tmp_path / "out" / "benchmark_manifest.csv")
    jsonl_rows = read_manifest(tmp_path / "out" / "benchmark_manifest.jsonl")
    assert [row["graph_id"] for row in csv_rows] == [row["graph_id"] for row in jsonl_rows]
