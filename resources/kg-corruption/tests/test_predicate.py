from __future__ import annotations

import random

from rdflib import Graph, Literal, Namespace

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.corruptions.predicate import PredicateCorruption
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.terms import term_to_json
from support import make_context

EX = Namespace("http://example.org/")


def _graph() -> Graph:
    graph = Graph()
    graph.add((EX.a, EX.knows, EX.b))
    graph.add((EX.a, EX.knows, EX.c))
    graph.add((EX.b, EX.knows, EX.c))
    graph.add((EX.a, EX.mentor, EX.d))
    graph.add((EX.a, EX.age, Literal("1")))
    graph.add((EX.b, EX.age, Literal("2")))
    return graph


def test_whole_predicates_removes_complete_groups():
    graph = _graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = PredicateCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=3,
        rng=random.Random(4),
        context=make_context(
            graph=graph,
            family_config={"mode": "whole_predicates", "predicate_sampling": "uniform"},
        ),
    )
    selected = set(result.metadata["selected_predicates"])
    assert selected
    for triple in elig.eligible:
        if term_to_json(triple[1]) in selected:
            assert triple in result.removed_triples
            assert triple not in result.retained_graph
    for triple in result.retained_graph:
        if triple in elig.eligible:
            assert term_to_json(triple[1]) not in selected


def test_within_predicates_hits_exact_count():
    graph = _graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = PredicateCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=2,
        rng=random.Random(1),
        context=make_context(
            graph=graph,
            family_config={"mode": "within_predicates"},
        ),
    )
    assert len(result.removed_triples) == 2
    selected = set(result.metadata["selected_predicates"])
    for triple in result.removed_triples:
        assert term_to_json(triple[1]) in selected
