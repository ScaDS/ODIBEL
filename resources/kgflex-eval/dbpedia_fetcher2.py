import requests
import os
import sys
from rdflib import Graph, URIRef
from urllib.parse import urlencode
from tqdm import tqdm

SPARQL_ENDPOINT = "http://localhost:8890/sparql"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/rdf+xml"
}

BASE_DIRS = {
    "raw": "entities/raw",
    "dbo": "entities/dbo",
    "dbp": "entities/dbp",
    "abstracts": "entities/film_abstracts"
}

SUBCATEGORIES = ["film", "person", "company"]

for key, base in BASE_DIRS.items():
    if key == "abstracts":
        os.makedirs(base, exist_ok=True)
    else:
        for sub in SUBCATEGORIES:
            os.makedirs(os.path.join(base, sub), exist_ok=True)

FILM_PROPERTIES = {
    "http://dbpedia.org/property/genre": "",
    "http://dbpedia.org/ontology/runtime": "",
    "http://dbpedia.org/ontology/budget": "",
    "http://dbpedia.org/property/gross": "",
    "http://dbpedia.org/ontology/title": "",
    "http://dbpedia.org/ontology/writer": "Person",
    "http://dbpedia.org/ontology/starring": "Person",
    "http://dbpedia.org/ontology/productionCompany": "Company",
    "http://dbpedia.org/ontology/director": "Person",
    "http://dbpedia.org/ontology/distributor": "Company",
    "http://dbpedia.org/ontology/producer": "Person",
    "http://dbpedia.org/ontology/cinematography": "Person",
    "http://dbpedia.org/ontology/musicComposer": "Person"
}

PERSON_PROPERTIES = {
    "http://dbpedia.org/ontology/birthDate": "",
    "http://dbpedia.org/ontology/deathDate": "",
    "http://dbpedia.org/ontology/birthPlace": "Place",
    "http://dbpedia.org/ontology/deathPlace": "Place",
    "http://dbpedia.org/ontology/occupation": "",
    "http://dbpedia.org/ontology/nationality": "Place",
    "http://dbpedia.org/ontology/spouse": "Person",
    "http://dbpedia.org/ontology/child": "Person",
    "http://dbpedia.org/ontology/award": ""
}

ORG_PROPERTIES = {
    "http://dbpedia.org/property/name": "",
    "http://dbpedia.org/ontology/foundingYear": "",
    "http://dbpedia.org/ontology/foundingDate": "",
    "https://dbpedia.org/property/products": "",
    "http://dbpedia.org/ontology/industry": "",
    "http://dbpedia.org/ontology/revenue": "",
    "http://dbpedia.org/ontology/numberOfEmployees": "",
    "http://dbpedia.org/ontology/headquarter": "Place"
}

PATH_NAV_PROPERTIES = {
    "http://dbpedia.org/ontology/genre",
    "http://dbpedia.org/ontology/birthPlace",
    "http://dbpedia.org/ontology/deathPlace",
    "http://dbpedia.org/ontology/nationality",
    "http://dbpedia.org/ontology/headquarter",
    "http://dbpedia.org/ontology/occupation"
}


visited_persons = set()
visited_companies = set()

def construct_query(subject_uri, properties):
    construct_lines = [
        f"<{subject_uri}> <http://www.w3.org/2000/01/rdf-schema#label> ?label .",
        f"<{subject_uri}> ?p ?o ."
    ]

    where_lines = [
        f"<{subject_uri}> <http://www.w3.org/2000/01/rdf-schema#label> ?label .",
        f"<{subject_uri}> ?p ?o .",
        "FILTER( strstarts(str(?p),'http://dbpedia.org/ontology/') || strstarts(str(?p),'http://dbpedia.org/property/') )"
    ]
    # Für Properties, die Pfadnavigation benötigen
    for prop in properties:
        if prop in PATH_NAV_PROPERTIES:
            label_var = prop.split("/")[-1]  # z. B. genre → ?genre
            construct_lines.append(f"<{subject_uri}> <{prop}> ?{label_var} .")
            where_lines.append(f"OPTIONAL {{ <{subject_uri}> <{prop}>/<http://www.w3.org/2000/01/rdf-schema#label> ?{label_var} . }}")


    construct_block = '\n'.join(construct_lines)
    where_block = '\n'.join(where_lines)

    query = f"""
    CONSTRUCT {{
    {construct_block}
    }}
    WHERE {{
    {where_block}
    }}
    """

    return query

def construct_filtered_query(uri, prefix):
    return f"""
    CONSTRUCT {{ <{uri}> ?p ?o . }}
    WHERE {{
        <{uri}> ?p ?o .
        FILTER(STRSTARTS(STR(?p), "{prefix}"))
    }}
    """

def run_query(query):
    payload = urlencode({'query': query})
    response = requests.post(SPARQL_ENDPOINT, data=payload, headers=HEADERS)

    graph = Graph()
    graph.parse(data=response.text, format="application/rdf+xml")
    return graph

def save_graph(graph, name, category, folder):
    filename = os.path.join(BASE_DIRS[folder], category, f"{name}.ttl")
    graph.serialize(destination=filename, format="turtle")

def fetch_entity_data(uri, properties, category, recursive=False):

    if category == "person" and visited_persons is not None:
        if uri in visited_persons:
            return
        visited_persons.add(uri)
    elif category == "company" and visited_companies is not None:
        if uri in visited_companies:
            return
        visited_companies.add(uri)

    name = uri.split("/")[-1]
    graph = run_query(construct_query(uri, properties))
    save_graph(graph, name, category, "raw")

    # Filtere Tripel nach Namespace und speichere getrennt
    dbo_ns = "http://dbpedia.org/ontology/"
    dbp_ns = "http://dbpedia.org/property/"

    dbo_graph = Graph()
    dbp_graph = Graph()

    for s, p, o in graph:
        if str(p) in properties.keys():
            if str(p).startswith(dbo_ns):
                dbo_graph.add((s, p, o))
            elif str(p).startswith(dbp_ns):
                dbp_graph.add((s, p, o))

    save_graph(dbo_graph, name, category, "dbo")
    save_graph(dbp_graph, name, category, "dbp")

    if category == "film":
        abstract_graph = Graph()
        for s, p, o in graph:
            if str(p).startswith("http://dbpedia.org/ontology/abstract"):
                abstract_graph.add((s, p, o))
        save_graph(abstract_graph, name, "", "abstracts")

    if recursive and category == "person":
        related = extract_related_entities(graph, PERSON_PROPERTIES)
        for rel_uri in related["persons"]:
            fetch_entity_data(rel_uri, PERSON_PROPERTIES, "person")

    if recursive and category == "company":
        related = extract_related_entities(graph, ORG_PROPERTIES)
        for rel_uri in related["persons"]:
            fetch_entity_data(rel_uri, PERSON_PROPERTIES, "person")

def extract_related_entities(graph, properties):
    persons = set()
    companies = set()
    for p_uri, obj_type in properties.items():
        for s, p, o in graph.triples((None, URIRef(p_uri), None)):
            if isinstance(o, URIRef):
                if obj_type == "Person":
                    persons.add(str(o))
                elif obj_type == "Company":
                    companies.add(str(o))
    return {"persons": persons, "companies": companies}

def main(uri_file_path):
    with open(uri_file_path, "r", encoding="utf-8") as f:
        film_uris = [line.strip() for line in f if line.strip()]

    all_persons = set()
    all_companies = set()

    for film_uri in tqdm(film_uris, desc="Fetching films"):
        fetch_entity_data(film_uri, FILM_PROPERTIES, "film")
        film_graph = run_query(construct_query(film_uri, FILM_PROPERTIES))
        entities = extract_related_entities(film_graph, FILM_PROPERTIES)
        all_persons.update(entities["persons"])
        all_companies.update(entities["companies"])

    for person in tqdm(all_persons, desc="Fetching persons"):
        fetch_entity_data(person, PERSON_PROPERTIES, "person", recursive=True)

    for company in tqdm(all_companies, desc="Fetching companies"):
        fetch_entity_data(company, ORG_PROPERTIES, "company", recursive=True)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dbpedia_fetcher2.py <path/to/film_uri_list.txt>")
        sys.exit(1)

    main(sys.argv[1])
