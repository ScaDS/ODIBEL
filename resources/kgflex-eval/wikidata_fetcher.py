import requests
import os
import sys
from rdflib import Graph, URIRef, Literal, RDFS
from urllib.parse import quote
from tqdm import tqdm

WIKIDATA_DIR = "/data/benchmark/3500/wikidata"
SUBCATEGORIES = ["film", "person", "company"]

for sub in SUBCATEGORIES:
    os.makedirs(os.path.join(WIKIDATA_DIR, sub), exist_ok=True)
    os.makedirs(os.path.join(WIKIDATA_DIR, "raw", sub), exist_ok=True)


WIKIDATA_FILM_PROPERTIES = {
    "http://www.wikidata.org/prop/direct/P136": "",
    "http://www.wikidata.org/prop/direct/P58": "Person",
    "http://www.wikidata.org/prop/direct/P161": "Person",
    "http://www.wikidata.org/prop/direct/P272": "Company",
    "http://www.wikidata.org/prop/direct/P57": "Person",
    "http://www.wikidata.org/prop/direct/P750": "Company",
    "http://www.wikidata.org/prop/direct/P2047": "",
    "http://www.wikidata.org/prop/direct/P1431": "",
    "http://www.wikidata.org/prop/direct/P2130": "",
    "http://www.wikidata.org/prop/direct/P2142": "",
    "http://www.wikidata.org/prop/direct/P344": "Person",
    "http://www.wikidata.org/prop/direct/P86": "Person",
    "http://www.wikidata.org/prop/direct/P1476": ""
}

WIKIDATA_PERSON_PROPERTIES = {
    "http://www.wikidata.org/prop/direct/P569": "",
    "http://www.wikidata.org/prop/direct/P570": "",
    "http://www.wikidata.org/prop/direct/P19": "Place",
    "http://www.wikidata.org/prop/direct/P20": "Place",
    "http://www.wikidata.org/prop/direct/P106": "",
    "http://www.wikidata.org/prop/direct/P27": "Place",
    "http://www.wikidata.org/prop/direct/P26": "Person",
    "http://www.wikidata.org/prop/direct/P40": "Person",
    "http://www.wikidata.org/prop/direct/P166": ""
}

WIKIDATA_ORG_PROPERTIES = {
    "http://www.wikidata.org/prop/direct/P1448": "",
    "http://www.wikidata.org/prop/direct/P571": "",
    "http://www.wikidata.org/prop/direct/P1056": "",
    "http://www.wikidata.org/prop/direct/P452": "",
    "http://www.wikidata.org/prop/direct/P2139": "",
    "http://www.wikidata.org/prop/direct/P1128": "",
    "http://www.wikidata.org/prop/direct/P159": "Place"
}

visited_persons = set()
visited_companies = set()

def sanitize_uri(uri):
    if isinstance(uri, URIRef):
        return URIRef(quote(str(uri), safe=':/#'))
    return uri

def sanitize_graph(graph):
    new_graph = Graph()
    for s, p, o in graph:
        s = sanitize_uri(s)
        p = sanitize_uri(p)
        o = sanitize_uri(o) if isinstance(o, URIRef) else o
        new_graph.add((s, p, o))
    return new_graph

def resolve_labels(graph, properties):
    resolved_graph = Graph()

    for s, p, o in graph:
        p_str = str(p)
        if p_str in properties and properties[p_str] == "":
            # Wenn o eine URI ist und kein Literal
            if isinstance(o, URIRef):
                # Suche das rdfs:label des Objekts
                label = None
                for _, _, label_val in graph.triples((o, RDFS.label, None)):
                    if isinstance(label_val, Literal):
                        label = label_val
                        break
                if label:
                    resolved_graph.add((s, p, label))
                    continue  # Überspringe Hinzufügen des ursprünglichen Tripels
        resolved_graph.add((s, p, o))

    return resolved_graph

def get_wikidata_entity(entity_uri, category, allowed_properties):
    HEADERS = {
        "Accept": "text/turtle"
    }

    response = requests.get(entity_uri, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Request failed: {response.status_code} - {entity_uri}")

    raw_graph = Graph()
    raw_graph.parse(data=response.text, format="turtle")

    entity_id = entity_uri.split("/")[-1]
    save_graph(raw_graph, entity_id, category, folder="raw")

    raw_graph = resolve_labels(raw_graph, allowed_properties)
    filtered = Graph()
    for s, p, o in raw_graph:
        if str(s).startswith(entity_uri) and str(p) in allowed_properties:
            filtered.add((s, p, o))

    return filtered


def save_graph(graph, name, category, folder):
    graph = sanitize_graph(graph)
    filename = os.path.join(WIKIDATA_DIR, folder, category, f"{name}.nt")
    graph.serialize(destination=filename, format="nt")

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

def fetch_entity_data(uri, properties, category, recursive=False):
    if category == "person":
        if uri in visited_persons:
            return
        visited_persons.add(uri)
    elif category == "company":
        if uri in visited_companies:
            return
        visited_companies.add(uri)

    try:
        graph = get_wikidata_entity(uri, category, properties)
    except Exception as e:
        print(f"Error fetching {uri}: {e}")
        return


    name = uri.split("/")[-1]
    save_graph(graph, name, category, "")

    if recursive and category == "person":
        related = extract_related_entities(graph, WIKIDATA_PERSON_PROPERTIES)
        for rel_uri in related["persons"]:
            fetch_entity_data(rel_uri, WIKIDATA_PERSON_PROPERTIES, "person")
    elif recursive and category == "company":
        related = extract_related_entities(graph, WIKIDATA_ORG_PROPERTIES)
        for rel_uri in related["persons"]:
            fetch_entity_data(rel_uri, WIKIDATA_PERSON_PROPERTIES, "person")

def main(uri_file_path):
    with open(uri_file_path, "r", encoding="utf-8") as f:
        film_uris = [line.strip().split()[1] for line in f if line.strip()]

    all_persons = set()
    all_companies = set()

    for film_uri in tqdm(film_uris, desc="Fetching films"):
        graph = get_wikidata_entity(film_uri, "film", WIKIDATA_FILM_PROPERTIES)
        name = film_uri.split("/")[-1]
        save_graph(graph, name, "film", "")

        entities = extract_related_entities(graph, WIKIDATA_FILM_PROPERTIES)
        all_persons.update(entities["persons"])
        all_companies.update(entities["companies"])

    for person_uri in tqdm(all_persons, desc="Fetching persons"):
        fetch_entity_data(person_uri, WIKIDATA_PERSON_PROPERTIES, "person", recursive=True)

    for company_uri in tqdm(all_companies, desc="Fetching companies"):
        fetch_entity_data(company_uri, WIKIDATA_ORG_PROPERTIES, "company", recursive=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python wikidata_fetcher.py <path/to/film_uri_list.txt>")
        sys.exit(1)

    main(sys.argv[1])