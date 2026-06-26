"""URI replacement and shading for RDF graphs (used by multi-source KG pipelines)."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import TYPE_CHECKING

from rdflib import Graph, URIRef
from rdflib.namespace import RDFS, SKOS

from pyodibel.operations.rdf.cluster import MatchCluster

if TYPE_CHECKING:
    from kgcore.model.ontology import Ontology


def hashed_resource_uri(uri: str, resource_namespace: str = "http://kg.org/resource/") -> str:
    """Hash a DBpedia resource URI into a shaded kg.org resource URI."""
    return resource_namespace + hashlib.md5(uri.encode()).hexdigest()


def replace_namespace(graph: Graph, namespace: str, equivalent_namespace: str) -> Graph:
    """Replace URI prefixes in subject, predicate, and object positions."""

    def replace_uri(uri: URIRef) -> URIRef:
        uri_str = str(uri)
        if uri_str.startswith(namespace):
            return URIRef(uri_str.replace(namespace, equivalent_namespace, 1))
        return uri

    new_graph = Graph()
    for subject, predicate, obj in graph:
        if isinstance(subject, URIRef):
            subject = replace_uri(subject)
        if isinstance(predicate, URIRef):
            predicate = replace_uri(predicate)
        if isinstance(obj, URIRef):
            obj = replace_uri(obj)
        new_graph.add((subject, predicate, obj))
    return new_graph


def replace_to_namespace(graph: Graph, clusters: MatchCluster, equivalent_namespace: str) -> Graph:
    """Replace URIs using ontology equivalence clusters."""

    def replace_uri(uri: URIRef) -> URIRef:
        uri_str = str(uri)
        matched = clusters.has_match_to_namespace(uri_str, equivalent_namespace)
        if matched is not None:
            return URIRef(matched)
        return uri

    new_graph = Graph()
    for subject, predicate, obj in graph:
        if isinstance(subject, URIRef):
            subject = replace_uri(subject)
        if isinstance(predicate, URIRef):
            predicate = replace_uri(predicate)
        if isinstance(obj, URIRef):
            obj = replace_uri(obj)
        new_graph.add((subject, predicate, obj))
    return new_graph


def replace_with_func_on_namespace(
    graph: Graph,
    func: Callable[[str], str],
    selected_namespace: str,
) -> Graph:
    """Replace URIs under ``selected_namespace`` using ``func``."""

    new_graph = Graph()
    for subject, predicate, obj in graph:
        if isinstance(subject, URIRef) and str(subject).startswith(selected_namespace):
            subject = URIRef(func(str(subject)))
        if isinstance(predicate, URIRef) and str(predicate).startswith(selected_namespace):
            predicate = URIRef(func(str(predicate)))
        if isinstance(obj, URIRef) and str(obj).startswith(selected_namespace):
            obj = URIRef(func(str(obj)))
        new_graph.add((subject, predicate, obj))
    return new_graph


def load_match_clusters_from_ontology(ontology: Ontology) -> MatchCluster:
    """Build match clusters from ontology class/property equivalents."""
    match_clusters = MatchCluster()

    for class_ in ontology.classes:
        for uri in class_.equivalent:
            match_clusters.add_match(class_.uri, uri)

    for property_ in ontology.properties:
        for uri in property_.equivalent:
            match_clusters.add_match(property_.uri, uri)

    return match_clusters


def copy_label_to_skos_alt_label(graph: Graph) -> Graph:
    """Copy ``rdfs:label`` values to ``skos:altLabel`` for provenance."""
    for subject, _, label in graph.triples((None, RDFS.label, None)):
        graph.add((subject, SKOS.altLabel, label))
    return graph


def shade_reference_graph(
    graph: Graph,
    *,
    resource_namespace: str = "http://kg.org/resource/",
    ontology_source_ns: str = "http://dbpedia.org/ontology/",
    ontology_target_ns: str = "http://kg.org/ontology/",
    resource_source_ns: str = "http://dbpedia.org/resource/",
    property_source_ns: str | None = "http://dbpedia.org/property/",
    clusters: MatchCluster | None = None,
    copy_labels: bool = True,
) -> Graph:
    """
    Shade a DBpedia-rooted reference graph into kg.org namespaces.

    Mirrors the post-processing in ``resources/movie-multi-source-kg/generate.py``:
    ontology/property URIs are canonicalized, resource URIs are MD5-hashed.
    """
    if clusters is not None:
        shaded = replace_to_namespace(graph, clusters, ontology_target_ns)
    else:
        shaded = replace_namespace(graph, ontology_source_ns, ontology_target_ns)
        if property_source_ns:
            shaded = replace_namespace(shaded, property_source_ns, ontology_target_ns)

    shaded = replace_with_func_on_namespace(
        shaded,
        lambda uri: hashed_resource_uri(uri, resource_namespace),
        resource_source_ns,
    )

    if copy_labels:
        shaded = copy_label_to_skos_alt_label(shaded)
    return shaded
