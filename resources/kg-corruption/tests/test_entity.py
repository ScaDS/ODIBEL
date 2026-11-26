from __future__ import annotations

import random

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDFS

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.corruptions.entity import EntityCorruption
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.terms import is_resource, term_to_json
from support import make_context

EX = Namespace("http://example.org/")


def _graph() -> Graph:
    graph = Graph()
    graph.add((EX.alice, RDFS.label, Literal("Alice")))
    graph.add((EX.alice, EX.knows, EX.bob))
    graph.add((EX.alice, EX.age, Literal("30")))
    graph.add((EX.bob, EX.knows, EX.carol))
    graph.add((EX.carol, EX.isolated, EX.nowhere))
    return graph


def _incident(entity, triple) -> bool:
    return triple[0] == entity or (is_resource(triple[2]) and triple[2] == entity)


def _selected_nodes(eligible, selected_ids: set[str]):
    nodes = {triple[0] for triple in eligible}
    nodes |= {triple[2] for triple in eligible if is_resource(triple[2])}
    return [node for node in nodes if term_to_json(node) in selected_ids]


def test_entity_incident_removes_all_incident_triples():
    graph = _graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = EntityCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=3,
        rng=random.Random(1),
        context=make_context(graph=graph, family_config={"mode": "incident"}),
    )
    selected = _selected_nodes(elig.eligible, set(result.metadata["selected_entities"]))
    assert selected
    for entity in selected:
        for triple in elig.eligible:
            if _incident(entity, triple):
                assert triple in result.removed_triples
                assert triple not in result.retained_graph


def test_entity_unrelated_triples_remain():
    graph = _graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = EntityCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=2,
        rng=random.Random(3),
        context=make_context(graph=graph, family_config={"mode": "incident"}),
    )
    selected = _selected_nodes(elig.eligible, set(result.metadata["selected_entities"]))
    for triple in elig.eligible:
        if all(not _incident(entity, triple) for entity in selected):
            assert triple in result.retained_graph
            assert triple not in result.removed_triples


def test_entity_subject_mode_only_removes_outgoing():
    graph = _graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = EntityCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=10,
        rng=random.Random(0),
        context=make_context(graph=graph, family_config={"mode": "subject"}),
    )
    selected_ids = set(result.metadata["selected_entities"])
    for triple in result.removed_triples:
        assert term_to_json(triple[0]) in selected_ids
        assert triple in elig.eligible
