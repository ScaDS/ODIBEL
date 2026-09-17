from __future__ import annotations

import random

from rdflib import Graph, Literal, Namespace, URIRef

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.corruptions.subgraph import SubgraphCorruption, _adjacency, _bfs
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.terms import term_to_json
from support import make_context

EX = Namespace("http://example.org/")


def _chain_graph() -> Graph:
    graph = Graph()
    graph.add((EX.c0, EX.next, EX.c1))
    graph.add((EX.c1, EX.next, EX.c2))
    graph.add((EX.c2, EX.next, EX.c3))
    graph.add((EX.c0, EX.note, Literal("seed")))
    return graph


def test_bfs_respects_hop_limit():
    graph = _chain_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    adj = _adjacency(elig.eligible, traversal="undirected")
    hop0 = _bfs(EX.c0, adj, 0)
    hop1 = _bfs(EX.c0, adj, 1)
    hop2 = _bfs(EX.c0, adj, 2)
    assert hop0 == {EX.c0}
    assert hop1 == {EX.c0, EX.c1}
    assert hop2 == {EX.c0, EX.c1, EX.c2}
    assert EX.c3 not in hop1


def test_subgraph_neighborhood_is_local():
    graph = _chain_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())
    result = SubgraphCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=2,
        rng=random.Random(2),
        context=make_context(
            graph=graph,
            family_config={"max_hops": 1, "include_literal_properties": True},
        ),
    )
    assert result.metadata["max_hops"] == 1
    seeds = result.metadata["seed_entities"]
    assert seeds
    # Every removed resource-object triple must touch a visited 1-hop ball of a seed.
    adj = _adjacency(elig.eligible, traversal="undirected")
    allowed = set()
    for seed_uri in seeds:
        seed = URIRef(seed_uri)
        allowed |= _bfs(seed, adj, 1)
    for triple in result.removed_triples:
        subject, _pred, obj = triple
        assert subject in allowed or obj in allowed


def test_subgraph_does_not_jump_beyond_hops_when_seed_is_endpoint():
    graph = _chain_graph()
    elig = build_eligibility_sets(graph, EligibilityConfig())

    class PreferC0:
        def shuffle(self, xs):
            xs.sort(key=lambda node: 0 if node == EX.c0 else 1)

    result = SubgraphCorruption().corrupt(
        graph,
        elig.eligible,
        target_removed_count=3,
        rng=PreferC0(),
        context=make_context(graph=graph, family_config={"max_hops": 1}),
    )
    assert result.metadata["seed_entities"][0] == str(EX.c0)
    far = (EX.c2, EX.next, EX.c3)
    # c2 is two hops from c0, so the far edge is only removed if it is incident
    # to a visited node (c1). Incident-to-visited can include (c1, next, c2).
    assert far not in result.removed_triples
    assert (EX.c0, EX.next, EX.c1) in result.removed_triples
