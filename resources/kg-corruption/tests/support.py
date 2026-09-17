from __future__ import annotations

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.eligibility import build_eligibility_sets
from kg_corrupt.model import CorruptionContext

EX = Namespace("http://example.org/")


def people_graph() -> Graph:
    """Tiny graph: two people, labels, and mixed eligible facts."""
    graph = Graph()
    graph.add((EX.alice, RDF.type, EX.Person))
    graph.add((EX.alice, RDFS.label, Literal("Alice")))
    graph.add((EX.alice, EX.knows, EX.bob))
    graph.add((EX.alice, EX.age, Literal("30")))
    graph.add((EX.bob, RDF.type, EX.Person))
    graph.add((EX.bob, RDFS.label, Literal("Bob")))
    graph.add((EX.bob, EX.knows, EX.carol))
    graph.add((EX.Person, RDF.type, OWL.Class))
    return graph


def make_context(*, graph=None, **kwargs) -> CorruptionContext:
    protected = kwargs.pop("protected_triples", None)
    eligibility_cfg = kwargs.pop("eligibility", EligibilityConfig())
    if protected is None and graph is not None:
        protected = build_eligibility_sets(graph, eligibility_cfg).protected
    return CorruptionContext(
        protected_triples=frozenset(protected or ()),
        family_config=kwargs.pop("family_config", {}),
        provenance=kwargs.pop("provenance", None),
        nested=kwargs.pop("nested", False),
        allow_blank_seed_entities=kwargs.pop("allow_blank_seed_entities", False),
    )
