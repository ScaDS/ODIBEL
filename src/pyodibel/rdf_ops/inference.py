from pathlib import Path
from rdflib import Graph, URIRef, RDF
from kgbench_extras.common.ontology import Ontology, OntologyUtil
# BIG TODO: change location of ontology code
from tqdm import tqdm

"""
enriches type information for entities in the RDF graph based on domain and range of relations
"""

BASE_DIR = Path("/home/marvin/project/odibel/data_1k/") # Path(__file__).parent

SEED_SOURCE_PATH = BASE_DIR / "seed.nt"
RDF_SOURCE_PATH = BASE_DIR / "source.nt"
REFERENCE_PATH = BASE_DIR / "full.nt"

ONTOLOGY_PATH = "/home/marvin/project/data/ontology.ttl"

def enrich_type_information(graph: Graph, ontology: Ontology, type_property: URIRef = RDF.type) -> Graph:
    type_dict = {}

    new_graph = Graph()

    for s, p, o in graph, desc="Enriching type information":
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




if __name__ == "__main__":
    ontology = OntologyUtil.load_ontology_from_file(ONTOLOGY_PATH)

    seed_graph = Graph()
    seed_graph.parse(SEED_SOURCE_PATH)
    seed_graph = enrich_type_information(seed_graph, ontology)
    seed_graph.serialize(destination=str(SEED_SOURCE_PATH)+".enriched.nt", format="nt")

    rdf_graph = Graph()
    rdf_graph.parse(RDF_SOURCE_PATH)
    rdf_graph = enrich_type_information(rdf_graph, ontology)
    rdf_graph.serialize(destination=str(RDF_SOURCE_PATH)+".enriched.nt", format="nt")

    reference_graph = Graph()
    reference_graph.parse(REFERENCE_PATH)
    reference_graph = enrich_type_information(reference_graph, ontology)
    reference_graph.serialize(destination=str(REFERENCE_PATH)+".enriched.nt", format="nt")