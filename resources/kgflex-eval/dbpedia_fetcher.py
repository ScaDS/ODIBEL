import requests
import os
import sys
from rdflib import Graph, URIRef

SPARQL_ENDPOINT = "https://dbpedia.org/sparql"
HEADERS = {'Accept': 'application/rdf+xml'}
OUTPUT_DIR = "entities"
os.makedirs(OUTPUT_DIR, exist_ok=True)

FILM_PROPERTIES = {

    "http://dbpedia.org/ontology/genre": "",
    "http://dbpedia.org/ontology/runtime": "",
    "http://dbpedia.org/ontology/budget": "",
    "http://dbpedia.org/property/gross": "",
    "http://dbpedia.org/ontology/title" : "",
    "http://dbpedia.org/ontology/writer": "Person",
    "http://dbpedia.org/ontology/starring": "Person",
    "http://dbpedia.org/ontology/productionCompany": "Company",
    "http://dbpedia.org/ontology/director": "Person",
    "http://dbpedia.org/ontology/distributor": "Company",
    "http://dbpedia.org/ontology/producer": "Person",
    "http://dbpedia.org/ontology/cinematography": "Person",
    "http://dbpedia.org/ontology/musicComposer": "Person"
}

PERSON_PROPERTIES = [
    "http://dbpedia.org/ontology/birthDate",
    "http://dbpedia.org/ontology/birthPlace",
    "http://dbpedia.org/ontology/deathDate",
    "http://dbpedia.org/ontology/deathPlace",
    "http://dbpedia.org/ontology/occupation",
    "http://dbpedia.org/ontology/nationality",
    "http://dbpedia.org/ontology/spouse",
    "http://dbpedia.org/ontology/child",
    "http://dbpedia.org/ontology/education",
    "http://dbpedia.org/ontology/award",
    "http://dbpedia.org/ontology/religion"
]

ORG_PROPERTIES = [
    "http://dbpedia.org/property/name",
    "http://dbpedia.org/ontology/foundingYear",
    "http://dbpedia.org/ontology/foundingDate",
    "https://dbpedia.org/property/products",
    "http://dbpedia.org/ontology/foundedBy",
    "http://dbpedia.org/ontology/industry",
    "http://dbpedia.org/ontology/parentCompany",
    "http://dbpedia.org/ontology/subsidiary",
    "http://dbpedia.org/ontology/ceo",
    "http://dbpedia.org/ontology/revenue",
    "http://dbpedia.org/ontology/numberOfEmployees"
]

def construct_query(subject_uri, properties):
    triple_patterns = "\n".join([f"<{subject_uri}> <{p}> ?o{i} ." for i, p in enumerate(properties)])
    return f"""
    CONSTRUCT {{
        {triple_patterns}
    }} WHERE {{
        {triple_patterns}
    }}
    """

def construct_entity_query(entity_uri, properties):
    triple_patterns = "\n".join([f"<{entity_uri}> <{p}> ?o{i} ." for i, p in enumerate(properties)])
    return f"""
    CONSTRUCT {{
        {triple_patterns}
    }} WHERE {{
        {triple_patterns}
    }}
    """

def run_query(query):
    response = requests.get(SPARQL_ENDPOINT, headers=HEADERS, params={
        "query": query,
        "format": "application/rdf+xml"
    })
    graph = Graph()
    graph.parse(data=response.text)
    return graph

def save_graph(graph, name):
    filename = os.path.join(OUTPUT_DIR, f"{name}.ttl")
    graph.serialize(destination=filename, format="turtle")

def fetch_film_data(film_uri):
    print(f"Fetching film data: {film_uri}")
    graph = run_query(construct_query(film_uri, FILM_PROPERTIES))
    save_graph(graph, "Film_Titanic")
    return graph

def extract_entities(graph):
    persons = set()
    companies = set()
    for p_uri, type_ in FILM_PROPERTIES.items():
        for s, p, o in graph.triples((None, URIRef(p_uri), None)):
            if isinstance(o, URIRef):
                if type_ == "Person":
                    persons.add(str(o))
                elif type_ == "Company":
                    companies.add(str(o))
    return persons, companies

def fetch_entities(uris, properties, label):
    for uri in uris:
        name = uri.split("/")[-1]
        print(f"Fetching {label}: {uri}")
        graph = run_query(construct_entity_query(uri, properties))
        save_graph(graph, f"{label}_{name}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dbpedia_fetcher.py <path/to/film_uri_list.txt>")
        sys.exit(1)

    uri_file_path = sys.argv[1]
    with open(uri_file_path, "r", encoding="utf-8") as f:
        film_uris = [line.strip() for line in f if line.strip()]

    for film_uri in film_uris:
        name = film_uri.split("/")[-1]
        film_graph = fetch_film_data(film_uri)
        persons, companies = extract_entities(film_graph)
        fetch_entities(persons, PERSON_PROPERTIES, "Person")
        fetch_entities(companies, ORG_PROPERTIES, "Company")
