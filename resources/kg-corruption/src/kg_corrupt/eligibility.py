"""Split a root graph into protected vs evaluation triples."""

from __future__ import annotations

from dataclasses import dataclass

from rdflib import Graph
from rdflib.namespace import RDF
from rdflib.term import URIRef

from kg_corrupt.config import EligibilityConfig
from kg_corrupt.model import Triple
from kg_corrupt.terms import (
    SCHEMA_PREDICATES,
    is_bnode,
    is_schema_namespace,
    triple_sort_key,
)


@dataclass(frozen=True)
class EligibilitySets:
    all_triples: tuple[Triple, ...]
    protected: frozenset[Triple]
    eligible: tuple[Triple, ...]

    @property
    def protected_count(self) -> int:
        return len(self.protected)

    @property
    def eligible_count(self) -> int:
        return len(self.eligible)


def is_schema_triple(triple: Triple) -> bool:
    _subject, predicate, obj = triple
    if predicate in SCHEMA_PREDICATES:
        return True
    if isinstance(predicate, URIRef) and is_schema_namespace(str(predicate)):
        if predicate != RDF.type:
            return str(predicate).startswith("http://www.w3.org/2002/07/owl#")
    if predicate == RDF.type and isinstance(obj, URIRef) and is_schema_namespace(str(obj)):
        return True
    return False


def is_protected_triple(triple: Triple, config: EligibilityConfig) -> bool:
    _subject, predicate, _obj = triple
    protected_predicates = {URIRef(value) for value in config.protected_predicates}
    if predicate in protected_predicates:
        return True
    if isinstance(predicate, URIRef):
        pred_str = str(predicate)
        for namespace in config.protected_namespaces:
            if pred_str.startswith(namespace):
                return True
    if config.protect_schema_triples and is_schema_triple(triple):
        return True
    if config.protect_blank_node_scaffolding and (
        is_bnode(triple[0]) or is_bnode(triple[2])
    ):
        return True
    return False


def build_eligibility_sets(graph: Graph, config: EligibilityConfig) -> EligibilitySets:
    all_triples = tuple(sorted((triple for triple in graph), key=triple_sort_key))
    protected = frozenset(
        triple for triple in all_triples if is_protected_triple(triple, config)
    )
    eligible = tuple(triple for triple in all_triples if triple not in protected)
    return EligibilitySets(
        all_triples=all_triples,
        protected=protected,
        eligible=eligible,
    )
