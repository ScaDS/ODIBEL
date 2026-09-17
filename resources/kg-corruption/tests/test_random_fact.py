from __future__ import annotations

import random

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.corruptions.random_fact import RandomFactCorruption
from kg_corrupt.eligibility import build_eligibility_sets
from support import make_context, people_graph


def _run(seed: int, target: int):
    graph = people_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = RandomFactCorruption().corrupt(
        graph,
        elig.eligible,
        target,
        random.Random(seed),
        make_context(graph=graph),
    )
    return graph, elig, result


def test_random_fact_removes_exact_requested_number():
    _graph, _elig, result = _run(1, 3)
    assert len(result.removed_triples) == 3
    assert result.metadata["realized_removed_count"] == 3


def test_random_fact_only_eligible_triples_removed():
    graph, elig, result = _run(2, 4)
    assert result.removed_triples <= set(elig.eligible)
    assert elig.protected <= set(result.retained_graph)
    assert set(result.retained_graph) <= set(graph)


def test_random_fact_deterministic_for_seed():
    _, _, first = _run(7, 2)
    _, _, second = _run(7, 2)
    assert first.removed_triples == second.removed_triples
    _, _, other = _run(8, 2)
    assert first.removed_triples != other.removed_triples
