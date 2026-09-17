from __future__ import annotations

from kg_corrupt.config import CorruptionConfig
from kg_corrupt.generator import corrupt_once, generate_one, prepare_root
from kg_corrupt.hashing import canonical_nt
from support import people_graph


def test_root_graph_never_changes(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    prepared = prepare_root(config)
    before = canonical_nt(prepared.root)
    corrupt_once(prepared, config, "random_fact", 0.5, 1)
    corrupt_once(prepared, config, "entity", 0.5, 2)
    corrupt_once(prepared, config, "long_tail", 0.4, 3)
    assert canonical_nt(prepared.root) == before


def test_corrupted_graph_is_subset(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    prepared = prepare_root(config)
    for family in ("random_fact", "entity", "predicate", "long_tail", "subgraph"):
        result = corrupt_once(prepared, config, family, 0.4, 9)
        assert set(result.retained_graph) <= set(prepared.root)
        assert not (set(result.retained_graph) - set(prepared.root))


def test_protected_triples_remain(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    prepared = prepare_root(config)
    assert prepared.eligibility.protected
    result = corrupt_once(prepared, config, "random_fact", 0.9, 1)
    assert prepared.eligibility.protected <= set(result.retained_graph)


def test_same_seed_same_triple_set(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    out_a = tmp_path / "a.nt"
    out_b = tmp_path / "b.nt"
    generate_one(config, family="random_fact", fraction=0.5, seed=42, output=out_a)
    generate_one(config, family="long_tail", fraction=0.5, seed=42, output=tmp_path / "lt_a.nt")
    generate_one(config, family="random_fact", fraction=0.5, seed=42, output=out_b)
    assert out_a.read_text(encoding="utf-8") == out_b.read_text(encoding="utf-8")


def test_nested_random_fact_is_monotonic(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    config.benchmark.nested = True
    prepared = prepare_root(config)
    small = corrupt_once(prepared, config, "random_fact", 0.2, 5)
    large = corrupt_once(prepared, config, "random_fact", 0.6, 5)
    assert small.removed_triples <= large.removed_triples
