from pathlib import Path
from rdflib import Graph, URIRef, RDF
from kgcore.model.ontology import Ontology
# BIG TODO: change location of ontology code
from tqdm import tqdm

"""
enriches type information for entities in the RDF graph based on domain and range of relations
"""

def enrich_type_information(graph: Graph, ontology: Ontology, type_property: URIRef = RDF.type) -> Graph:
    type_dict = {}

    new_graph = Graph()

    for s, p, o in graph:
        domain, range = ontology.get_domain_range(str(p))
        if domain and isinstance(s, URIRef):
            if str(s) not in type_dict:
                type_dict[str(s)] = []
            type_dict[str(s)].append(str(domain))   
        if range and isinstance(o, URIRef):
            if str(o) not in type_dict:
                type_dict[str(o)] = []
            type_dict[str(o)].append(str(range))
        new_graph.add((s, p, o))

    for uri, types in type_dict.items():
        for type in types:
            new_graph.add((URIRef(uri), type_property, URIRef(type)))
    return new_graph
