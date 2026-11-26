"""RDF term helpers used across I/O, eligibility, and strategies."""

from __future__ import annotations

from rdflib.namespace import OWL, RDF, RDFS
from rdflib.term import BNode, Literal, Node, URIRef

from kg_corrupt.model import Triple

OWL_NS = str(OWL)
RDFS_NS = str(RDFS)
RDF_NS = str(RDF)

SCHEMA_PREDICATES = frozenset(
    {
        RDFS.domain,
        RDFS.range,
        RDFS.subClassOf,
        RDFS.subPropertyOf,
        RDFS.comment,
        RDFS.isDefinedBy,
        RDFS.seeAlso,
        RDF.first,
        RDF.rest,
        RDF.subject,
        RDF.predicate,
        RDF.object,
    }
)


def is_literal(node: Node) -> bool:
    return isinstance(node, Literal)


def is_bnode(node: Node) -> bool:
    return isinstance(node, BNode)


def is_resource(node: Node) -> bool:
    return isinstance(node, (URIRef, BNode))


def term_to_nt(node: Node) -> str:
    return node.n3()


def triple_to_nt(triple: Triple) -> str:
    subject, predicate, obj = triple
    return f"{subject.n3()} {predicate.n3()} {obj.n3()} ."


def triple_sort_key(triple: Triple) -> str:
    return triple_to_nt(triple)


def term_to_json(node: Node) -> str:
    if isinstance(node, URIRef):
        return str(node)
    if isinstance(node, BNode):
        return f"_{node}"
    return node.n3()


def namespace_of(node: Node) -> str | None:
    if not isinstance(node, URIRef):
        return None
    value = str(node)
    hash_idx = value.rfind("#")
    slash_idx = value.rfind("/")
    cut = max(hash_idx, slash_idx)
    if cut < 0:
        return value
    return value[: cut + 1]


def is_schema_namespace(uri: str) -> bool:
    return uri.startswith(OWL_NS) or uri.startswith(RDFS_NS) or uri.startswith(RDF_NS)
