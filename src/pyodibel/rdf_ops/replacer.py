# Replace (map) URIs in a graph

from rdflib import Graph, URIRef, Literal, RDF, OWL
from kgbench.evaluation.cluster import MatchCluster
from kgbench_extras.common.ontology import Ontology, OntologyUtil
from pathlib import Path

def __load_match_clusters_from_Ontology(ontology: Ontology) -> MatchCluster:
    """
    Load match clusters from an ontology file.
    """
    match_clusters = MatchCluster()

    for class_ in ontology.classes:
        for uri in class_.equivalent:
            match_clusters.add_match(class_.uri, uri)

    for property_ in ontology.properties:
        for uri in property_.equivalent:
            match_clusters.add_match(property_.uri, uri)

    return match_clusters

def replace_namespace(graph: Graph, namespace: str, equivalent_namespace: str) -> Graph:
    """
    Replace all URIs in a graph that start with a given namespace with the equivalent namespace.
    """
    def replace_uri(uri: URIRef) -> URIRef:
        uri_str = str(uri)
        if uri_str.startswith(namespace):
            return URIRef(uri_str.replace(namespace, equivalent_namespace))
        return uri
    
    new_graph = Graph()

    for s, p, o in graph:
        if isinstance(s, URIRef):
            s = replace_uri(s)
        if isinstance(p, URIRef):
            p = replace_uri(p)
        if isinstance(o, URIRef):
            o = replace_uri(o)
        new_graph.add((s, p, o))
    
    return new_graph

def replace_to_namespace(graph: Graph, clusters: MatchCluster, equivalent_namespace: str) -> Graph:
    """
    Replace all URIs in a graph that start with a given namespace with the equivalent namespace.
    """

    def replace_uri(uri: URIRef) -> URIRef:
        uri_str = str(uri)
        opt_uri = clusters.has_match_to_namespace(uri_str, equivalent_namespace)
        if opt_uri is not None:
            return URIRef(opt_uri)
        return uri

    new_graph = Graph()

    for s, p, o in graph:
        if isinstance(s, URIRef):
            s = replace_uri(s)
        if isinstance(p, URIRef):
            p = replace_uri(p)
        if isinstance(o, URIRef):
            o = replace_uri(o)
        new_graph.add((s, p, o))

    return new_graph


def test_replace_namespace():
    graph = Graph()
    graph.parse("/home/marvin/project/data/acquisiton/film1k_bundle/split_0/kg/seed/data.nt", format="nt")
    graph = replace_namespace(graph, "http://dbpedia.org/ontology/", "http://kg.org/seed/ontology/")
    print(graph.serialize(format="nt"))

def test_replace_to_namespace():
    graph = Graph()
    graph.parse("/home/marvin/project/data/acquisiton/film1k_bundle/split_0/kg/seed/data.nt", format="nt")
    ontology = OntologyUtil.load_ontology_from_file(Path("/home/marvin/project/code/experiments/movie-ontology.ttl"))
    match_clusters = __load_match_clusters_from_Ontology(ontology)

    print(match_clusters)

    graph = replace_to_namespace(graph, match_clusters, "http://kg.org/ontology/")
    print(graph.serialize(format="nt"))

if __name__ == "__main__":
    test_replace_namespace()
    test_replace_to_namespace()

