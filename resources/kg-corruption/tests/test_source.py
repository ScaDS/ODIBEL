from __future__ import annotations

from collections import defaultdict

from rdflib import Dataset, Graph, Literal, URIRef

from kg_corrupt.config import CorruptionConfig, EligibilityConfig
from kg_corrupt.corruptions.source import SourceCorruption, _triples_removed_by_policy
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.generator import generate_one
from kg_corrupt.model import Triple
from kg_corrupt.provenance import ProvenanceError, ProvenanceIndex
from support import EX, make_context, people_graph
import pytest


def _index(mapping: dict[Triple, set[str]]) -> ProvenanceIndex:
    source_to_triples: dict[str, set[Triple]] = defaultdict(set)
    frozen = {triple: frozenset(ids) for triple, ids in mapping.items()}
    for triple, ids in frozen.items():
        for source_id in ids:
            source_to_triples[source_id].add(triple)
    return ProvenanceIndex(triple_to_sources=frozen, source_to_triples=dict(source_to_triples))


class ScriptedRng:
    def __init__(self, order):
        self.order = list(order)

    def shuffle(self, xs):
        rank = {item: i for i, item in enumerate(self.order)}
        xs.sort(key=lambda item: rank.get(item, len(rank)))


def test_source_removes_whole_units():
    graph = people_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    t_alice_knows = (EX.alice, EX.knows, EX.bob)
    t_alice_age = (EX.alice, EX.age, Literal("30"))
    t_bob_knows = (EX.bob, EX.knows, EX.carol)
    prov = _index(
        {
            t_alice_knows: {"doc_a"},
            t_alice_age: {"doc_a"},
            t_bob_knows: {"doc_b"},
        }
    )
    result = SourceCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=2,
        rng=ScriptedRng(["doc_a", "doc_b"]),
        context=make_context(graph=graph, provenance=prov),
    )
    assert t_alice_knows in result.removed_triples
    assert t_alice_age in result.removed_triples
    assert t_bob_knows not in result.removed_triples
    assert result.metadata["selected_sources"] == ["doc_a"]


def test_multi_source_policy_all_vs_any():
    graph = Graph()
    shared = (EX.alice, EX.knows, EX.bob)
    only_a = (EX.alice, EX.age, Literal("30"))
    only_b = (EX.bob, EX.age, Literal("42"))
    graph.add(shared)
    graph.add(only_a)
    graph.add(only_b)
    elig = build_eligibility_sets(graph, EligibilityConfig())
    prov = _index(
        {
            shared: {"doc_a", "doc_b"},
            only_a: {"doc_a"},
            only_b: {"doc_b"},
        }
    )
    eligible = set(elig.eligible)
    all_policy = _triples_removed_by_policy(eligible, prov, {"doc_a"}, "remove_if_all_selected")
    any_policy = _triples_removed_by_policy(eligible, prov, {"doc_a"}, "remove_if_any_selected")
    assert only_a in all_policy
    assert shared not in all_policy
    assert shared in any_policy
    assert only_a in any_policy
    assert only_b not in any_policy

    both = _triples_removed_by_policy(
        eligible, prov, {"doc_a", "doc_b"}, "remove_if_all_selected"
    )
    assert shared in both


def test_source_family_fails_without_provenance(tmp_path):
    graph = people_graph()
    path = tmp_path / "root.ttl"
    graph.serialize(path.as_posix(), format="turtle")
    config = CorruptionConfig()
    config.input.root_reference = path
    config.families.source.enabled = True
    with pytest.raises(ProvenanceError, match="no provenance mapping"):
        generate_one(
            config,
            family="source",
            fraction=0.3,
            seed=1,
            output=tmp_path / "out.nt",
        )


def test_named_graph_provenance(tmp_path):
    dataset = Dataset()
    g1 = URIRef("http://example.org/graph/doc_a")
    g2 = URIRef("http://example.org/graph/doc_b")
    dataset.get_context(g1).add((EX.alice, EX.knows, EX.bob))
    dataset.get_context(g2).add((EX.alice, EX.age, Literal("30")))
    path = tmp_path / "root.trig"
    dataset.serialize(path.as_posix(), format="trig")
    config = CorruptionConfig()
    config.input.root_reference = path
    config.input.format = "trig"
    config.families.source.enabled = True
    config.families.source.named_graph_as_source = True
    row = generate_one(
        config,
        family="source",
        fraction=0.5,
        seed=1,
        output=tmp_path / "out.nt",
    )
    assert row["realized_removed_count"] >= 1

