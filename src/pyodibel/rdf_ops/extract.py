from rdflib import URIRef, Graph

def extract_subgraph_recursive(root_entity: str, rdf_graph: Graph, visited=None):
    """
    Recursively extracts all triples from rdf_graph starting from root_entity.
    Follows URIRefs in object positions and avoids cycles.
    
    Parameters:
        root_entity (str): URI of the root entity (as a string).
        rdf_graph (rdflib.Graph): Source RDF graph.
        visited (set): Internal use to track visited URIs.
    
    Returns:
        rdflib.Graph: A new RDF graph with the extracted subgraph.
    """
    if visited is None:
        visited = set()

    subgraph = Graph()
    root_uri = URIRef(root_entity)

    if root_uri in visited:
        return subgraph

    visited.add(root_uri)

    for predicate, obj in rdf_graph.predicate_objects(subject=root_uri):
        subgraph.add((root_uri, predicate, obj))

        if isinstance(obj, URIRef):
            nested_subgraph = extract_subgraph_recursive(str(obj), rdf_graph, visited)
            subgraph += nested_subgraph

    return subgraph