from __future__ import annotations

from rdflib import BNode, Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.eligibility import build_eligibility_sets

EX = Namespace("http://example.org/")


def test_labels_and_schema_are_protected():
    graph = Graph()
    graph.add((EX.Person, RDF.type, OWL.Class))
    graph.add((EX.knows, RDFS.domain, EX.Person))
    graph.add((EX.alice, RDFS.label, Literal("Alice")))
    graph.add((EX.alice, EX.knows, EX.bob))
    elig = build_eligibility_sets(graph, EligibilityConfig())
    assert (EX.alice, RDFS.label, Literal("Alice")) in elig.protected
    assert (EX.Person, RDF.type, OWL.Class) in elig.protected
    assert (EX.knows, RDFS.domain, EX.Person) in elig.protected
    assert (EX.alice, EX.knows, EX.bob) in elig.eligible
    assert elig.eligible_count + elig.protected_count == len(elig.all_triples)


def test_protected_predicates_are_configurable():
    graph = Graph()
    graph.add((EX.alice, EX.knows, EX.bob))
    graph.add((EX.alice, RDFS.label, Literal("Alice")))
    cfg = EligibilityConfig(protected_predicates=[str(EX.knows)], protect_schema_triples=False)
    elig = build_eligibility_sets(graph, cfg)
    assert (EX.alice, EX.knows, EX.bob) in elig.protected
    assert (EX.alice, RDFS.label, Literal("Alice")) in elig.eligible


def test_blank_node_scaffolding_protection():
    graph = Graph()
    blank = BNode("n1")
    graph.add((EX.alice, EX.note, blank))
    graph.add((blank, EX.text, Literal("x")))
    graph.add((EX.alice, EX.knows, EX.bob))
    off = build_eligibility_sets(graph, EligibilityConfig(protect_blank_node_scaffolding=False))
    on = build_eligibility_sets(graph, EligibilityConfig(protect_blank_node_scaffolding=True))
    assert (EX.alice, EX.note, blank) in off.eligible
    assert (EX.alice, EX.note, blank) in on.protected
    assert (EX.alice, EX.knows, EX.bob) in on.eligible


def test_eligibility_order_is_deterministic():
    graph = Graph()
    graph.add((EX.b, EX.p, EX.o2))
    graph.add((EX.a, EX.p, EX.o1))
    first = build_eligibility_sets(graph, EligibilityConfig())
    second = build_eligibility_sets(graph, EligibilityConfig())
    assert first.eligible == second.eligible
