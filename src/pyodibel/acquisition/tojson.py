from rdflib import Graph, URIRef, RDFS
from urllib.parse import unquote, quote
import json, os
from hashlib import sha256

def build_recursive_json(subject, graph, visited=None):
    if visited is None:
        visited = set()
    if subject in visited:
        return None  # Prevent infinite loops

    visited.add(subject)
    result = {}

    # print(s_uri)

    # for s, p, o in graph:
    #     print(s, p, o)

    for _, p, o in graph.triples((URIRef(subject), None, None)):
        key = p.split("#")[-1] if "#" in p else p.split("/")[-1]

        def get_value(obj):
            if isinstance(obj, URIRef):
                return build_recursive_json(str(obj), graph, visited) 
                # else:
                #     # Just get label
                #     label = None
                #     for _, _, lbl in graph.triples((obj, RDFS.label, None)):
                #         label = str(lbl)
                #         break
                #     return {"uri": str(obj), "label": label or str(obj)}
            else:
                return str(obj)

        value = get_value(o)

        # Handle multiple values
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            if value and value != "" and value != {}:
                result[key].append(value)
        else:
            if value and value != "" and value != {}:
                result[key] = value

        result["provenance_hash"] = sha256(subject.encode("utf-8")).hexdigest()

    return result

def dbr_uri_from_filename(file_name: str) -> str:
    return "http://dbpedia.org/resource/" + os.path.basename(file_name)

def get_movie_uris(dir):
    movie_uris = []
    for dirpath, _, filenames in os.walk(dir):
        for filename in filenames:
            movie_uris.append(dbr_uri_from_filename(filename))
    return movie_uris

g = Graph()
g.parse("data/final/tmp-json.nt", format="nt")

# print(g.serialize(format="ntriples"))

movie_dirs = [
    "data/final/split_1/dbp/film",
    "data/final/split_2/dbp/film",
    "data/final/split_4/dbp/film"
]

movie_uris = []
for movie_dir in movie_dirs:
    movie_uris += get_movie_uris(movie_dir)


os.makedirs("data/final/json", exist_ok=True)

for uri in list(movie_uris):
    # print(uri)
    jsondata = build_recursive_json(quote(uri, safe=":/#"), g)

    with open(f"data/final/json/{uri.split('/')[-1]}.json", "w") as f:
        json.dump(jsondata, f, indent=4)
