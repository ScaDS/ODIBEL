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
            if value and value != "" and value != {} and not value in result[key]:
                result[key].append(value)
        else:
            if value and value != "" and value != {}:
                result[key] = value

        # result["provenance_hash"] = sha256(subject.encode("utf-8")).hexdigest()

    return result