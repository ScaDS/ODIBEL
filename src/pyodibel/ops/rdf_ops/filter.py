import hashlib
from rdflib import Graph

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()

def read_uris_from_file(file_path: str) -> list[str]:
    """Read URIs from a file."""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def filter_subject(graph: Graph, subject: str) -> Graph:
    """Filter a graph by subject."""
    filtered_graph = Graph()
    for s, p, o in graph:
        if str(s) == subject:
            filtered_graph.add((s, p, o))
    return filtered_graph

def filter_predicate(graph: Graph, predicate: str) -> Graph:
    """Filter a graph by predicate."""
    filtered_graph = Graph()
    for s, p, o in graph:
        if str(p) == predicate:
            filtered_graph.add((s, p, o))
    return filtered_graph

def filter_type_statement(graph: Graph, types: list[str]) -> Graph:
    """Filter a graph by type statement."""
    filtered_graph = Graph()
    for s, p, o in graph:
        if str(o) in types:
            filtered_graph.add((s, p, o))
    return filtered_graph

def filter_graph_by_namespaces(graph: Graph, namespaces: list[str]) -> Graph:
    """Filter a graph by namespaces."""
    filtered_graph = Graph()
    for s, p, o in graph:
        if any(str(s).startswith(namespace) for namespace in namespaces):
            filtered_graph.add((s, p, o))
    return filtered_graph

def union_graphs(graphs: list[Graph]) -> Graph:
    """Union a list of graphs."""
    union_graph = Graph()
    for graph in graphs:
        for triple in graph:
            union_graph.add(triple)
    return union_graph

