from __future__ import annotations

import random

from rdflib import Graph, Literal, Namespace

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.corruptions.long_tail import LongTailCorruption
from kg_corrupt.eligibility import build_eligibility_sets
from support import make_context

EX = Namespace("http://example.org/")


def _skewed_graph(n_common: int = 20, n_rare: int = 1) -> Graph:
    graph = Graph()
    for i in range(n_common):
        graph.add((EX[f"s{i}"], EX.common, Literal(str(i))))
    for i in range(n_rare):
        graph.add((EX[f"r{i}"], EX.rare, Literal(str(i))))
    return graph


def test_long_tail_removes_exact_requested_number():
    graph = _skewed_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = LongTailCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=5,
        rng=random.Random(11),
        context=make_context(graph=graph, family_config={"alpha": 1.0}),
    )
    assert len(result.removed_triples) == 5


def test_long_tail_deterministic():
    graph = _skewed_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    kwargs = dict(
        graph=graph,
        eligible_triples=elig.eligible,
        target_removed_count=4,
        context=make_context(graph=graph),
    )
    a = LongTailCorruption().corrupt(rng=random.Random(3), **kwargs)
    b = LongTailCorruption().corrupt(rng=random.Random(3), **kwargs)
    c = LongTailCorruption().corrupt(rng=random.Random(4), **kwargs)
    assert a.removed_triples == b.removed_triples
    assert a.removed_triples != c.removed_triples


def test_long_tail_prefers_rare_predicates():
    graph = _skewed_graph(n_common=24, n_rare=2)
    elig = build_eligibility_sets(graph, EligibilityConfig())
    rare_removed = 0
    common_removed = 0
    rare_total = 0
    common_total = 0
    for seed in range(40):
        result = LongTailCorruption().corrupt(
            graph,
            elig.eligible,
            target_removed_count=6,
            rng=random.Random(seed),
            context=make_context(graph=graph, family_config={"alpha": 1.0}),
        )
        for triple in elig.eligible:
            is_rare = triple[1] == EX.rare
            if is_rare:
                rare_total += 1
                rare_removed += int(triple in result.removed_triples)
            else:
                common_total += 1
                common_removed += int(triple in result.removed_triples)
    rare_rate = rare_removed / rare_total
    common_rate = common_removed / common_total
    assert rare_rate > common_rate
    assert result.metadata["mean_predicate_frequency_removed"] < result.metadata[
        "mean_predicate_frequency_retained"
    ] or rare_rate > 0.5
