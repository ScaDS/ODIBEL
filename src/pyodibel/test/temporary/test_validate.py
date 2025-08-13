from pathlib import Path
from random import seed
from rdflib import Graph, RDF, URIRef
from typing import Set, Tuple, Dict

from pyodibel.rdf.shade import reshaded_graph, reshade_entities, hash_uri
from rdflib.plugins.sparql.processor import prepareUpdate
from kgbench.common import KG, Data, DataFormat

from kgbench.evaluation.aspects.semantic import ReasoningMetric

BASE_DIR = Path("/home/marvin/project/data/current") # Path(__file__).parent

SEED_SOURCE_PATH = BASE_DIR / "sources/shade_seed.nt.enriched.nt"
RDF_SOURCE_PATH = BASE_DIR / "sources/shade_rdf_source.nt.enriched.nt"
ONTOLOGY_PATH = BASE_DIR / "ontology.ttl"

def check_missing_rdf_type(source_path: Path, type_property: str = RDF.type) -> Tuple[Set[str], Set[str]]:
    """
    Check if the RDF type is missing for the entity
    """
    g = Graph()
    g.parse(source_path, format="nt")
    
    entities = set()
    entities_with_rdf_type = set()

    for s, p, o in g:
        if isinstance(s, URIRef):
            entities.add(str(s))
        if isinstance(o, URIRef):
            entities.add(str(o))
    
    for s, p, o in g.triples((None, type_property, None)):
        if isinstance(s, URIRef):
            entities_with_rdf_type.add(str(s))
    
    return entities, entities_with_rdf_type


def get_uri_to_old_uri_mapping(source_path: Path, new_namespace: str) -> Dict[str, str]:
    """
    Get the URI to old URI mapping for the source
    """
    d = {}
    with open(source_path, "r") as f:
        for line in f:
            d[new_namespace+hash_uri(line.strip())] = line.strip()
    return d
    

def test_missing_rdf_type():
    """
    Test that the RDF type is missing for the entity
    """

    uri_to_old_uri_mapping = get_uri_to_old_uri_mapping("/home/marvin/project/data/current/full.nt.entities.txt", "http://kg.org/seed/")
    # print 10
    # for uri in list(uri_to_old_uri_mapping.keys())[:10]:
    #     print(uri)
    
    entities, entities_with_rdf_type = check_missing_rdf_type(SEED_SOURCE_PATH, type_property=URIRef("http://kg.org/seed/c74e2b735dd8dc85ad0ee3510c33925f"))
    print(f"SEED_SOURCE_PATH is type complete: {len(entities) - len(entities_with_rdf_type)}")
    # missing_entities_list = list(entities - entities_with_rdf_type)
    # missing_entities_list.sort()
    # for entity in missing_entities_list[:10]:
    #     try:
    #         print(uri_to_old_uri_mapping[entity], entity)
    #     except KeyError:
    #         print(f"KeyError: {entity}")
    entities, entities_with_rdf_type = check_missing_rdf_type(RDF_SOURCE_PATH, type_property=URIRef("http://kg.org/rdf/c74e2b735dd8dc85ad0ee3510c33925f"))
    print(f"RDF_SOURCE_PATH is type complete: {len(entities) - len(entities_with_rdf_type)}")

def test_kg_consistency():
    """
    Test that the KG is consistent
    """
    seed_kg = KG(id="seed", name="path", path=SEED_SOURCE_PATH, format=DataFormat.RDF_NTRIPLES)
    rdf_kg = KG(id="rdf", name="path", path=RDF_SOURCE_PATH, format=DataFormat.RDF_NTRIPLES)
    ontology_graph = Graph()
    ontology_graph.parse(ONTOLOGY_PATH)
    seed_kg.set_ontology_graph(ontology_graph)
    rdf_kg.set_ontology_graph(ontology_graph)
    metric_result = ReasoningMetric().compute(seed_kg)
    print(metric_result)
    metric_result = ReasoningMetric().compute(rdf_kg)
    print(metric_result)




if __name__ == "__main__":
    test_missing_rdf_type()
    test_kg_consistency()

