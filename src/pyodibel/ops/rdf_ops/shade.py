import hashlib
import json
from typing import Callable, Dict, List, Tuple
from rdflib import Graph, URIRef
from tqdm import tqdm

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


def shade_graph(graph: Graph, new_namespace: str) -> Tuple[Graph, Dict[str, str]]:
    """Shade a graph."""
    new_graph = Graph()
    new_to_old_uri: Dict[str, str] = {}
    for triple in tqdm(graph):
        subj, pred, obj = triple
        # Only shade if subject is a URIRef and predicate is a URIRef
        if isinstance(subj, URIRef) and isinstance(pred, URIRef):
            new_subj = URIRef(new_namespace + hash_uri(str(subj)))
            new_pred = URIRef(new_namespace + hash_uri(str(pred)))
            if isinstance(obj, URIRef):
                new_obj = URIRef(new_namespace + hash_uri(str(obj)))
                new_to_old_uri[str(new_obj)] = str(obj)
                new_graph.add((new_subj, new_pred, new_obj))
            else:
                new_obj = obj
            new_to_old_uri[str(new_subj)] = str(subj)
            new_to_old_uri[str(new_pred)] = str(pred)
            new_graph.add((new_subj, new_pred, new_obj))
        else:
            new_graph.add(triple)
    return new_graph, new_to_old_uri

def shade_graph_func(graph: Graph, new_namespace: str, func: Callable[[str], str], ignored_predicates: List[str] = []) -> Tuple[Graph, Dict[str, str]]:
    """Shade a graph."""
    new_graph = Graph()
    new_to_old_uri: Dict[str, str] = {}
    for triple in tqdm(graph):
        subj, pred, obj = triple
        # Only shade if subject is a URIRef and predicate is a URIRef
        if isinstance(subj, URIRef) and isinstance(pred, URIRef):
            new_subj = URIRef(new_namespace + func(str(subj)))
            new_pred = URIRef(new_namespace + func(str(pred))) if pred not in ignored_predicates else pred
            if isinstance(obj, URIRef):
                new_obj = URIRef(new_namespace + func(str(obj)))
                new_to_old_uri[str(new_obj)] = str(obj)
                new_graph.add((new_subj, new_pred, new_obj))
            else:
                new_obj = obj
            new_to_old_uri[str(new_subj)] = str(subj)
            new_to_old_uri[str(new_pred)] = str(pred)
            new_graph.add((new_subj, new_pred, new_obj))
        else:
            new_graph.add(triple)
    return new_graph, new_to_old_uri

def reshade_hash_uri_graph(graph: Graph, hash_to_uri: Dict[str, str]) -> Graph:
    """Reshade a graph."""

    def get_hash_part(uri: str) -> str:
        return uri.split("/")[-1]

    new_graph = Graph()
    for triple in graph:
        subj, pred, obj = triple

        new_subj = URIRef(hash_to_uri[get_hash_part(str(subj))]) if get_hash_part(str(subj)) in hash_to_uri else subj
        new_pred = URIRef(hash_to_uri[get_hash_part(str(pred))]) if get_hash_part(str(pred)) in hash_to_uri else pred
        if isinstance(obj, URIRef):
            new_obj = URIRef(hash_to_uri[get_hash_part(str(obj))]) if get_hash_part(str(obj)) in hash_to_uri else obj
            new_graph.add((new_subj, new_pred, new_obj))
        else:
            new_graph.add((new_subj, new_pred, obj))
    return new_graph

def reshaded_graph(graph: Graph, new_namespace: str, old_to_new_uri: Dict[str, str]) -> Graph:
    """Reshade a graph."""
    new_graph = Graph()
    for triple in graph:
        subj, pred, obj = triple
        if isinstance(subj, URIRef) and isinstance(pred, URIRef):
            new_subj = URIRef(new_namespace + old_to_new_uri[str(subj)])
        

def reshade_entities(entities: List[str], new_namespace: str, old_to_new_uri: Dict[str, str]) -> List[str]:
    """Reshade a list of entities."""
    return [new_namespace + hash_uri(entity) for entity in entities]

def read_shade_mapping(path: str) -> Dict[str, str]:
    """Read the shade mapping from a JSON file."""
    with open(path, "r") as f:
        return json.load(f)

# def reshade_graph(graph: Graph, old_namespace: str) -> Graph:
#     """Reshade a graph."""
#     new_graph = Graph()
#     for triple in graph:
#         subj, pred, obj = triple
#         # Only shade if subject is a URIRef and predicate is a URIRef
#         if isinstance(subj, URIRef) and isinstance(pred, URIRef):
#             new_subj = URIRef(old_namespace + str(subj))
if __name__ == "__main__":

    uri_to_hash_tsv = "/home/marvin/project/data/films_1k/testing/uri_to_md5.tsv"
    hash_to_uri = {}
    with open(uri_to_hash_tsv, "r") as f:
        for line in f:
            uri, hash = line.strip().split("\t")
            hash_to_uri[hash] = uri

    # graph = reshade_hash_uri_graph(Graph().parse("/home/marvin/project/data/films_1k/results/json_a/tmp/5_generate_rdf_json_task_0.nt", format="nt"), hash_to_uri)
    # graph.serialize("/home/marvin/project/data/films_1k/results/json_a/tmp/5_generate_rdf_json_task_0.nt.reshade.nt", format="nt")


    graph = reshade_hash_uri_graph(Graph().parse("/home/marvin/project/data/films_1k/sources/seed.nt", format="nt"), hash_to_uri)
    graph.serialize("/home/marvin/project/data/films_1k/sources/seed.nt.reshade.nt", format="nt")

    # def get_suffix(uri: str) -> str:
    #     return uri.split("/")[-1]

    # rdf_source = "/home/marvin/project/odibel/data_1k/build/source.nt.enriched.nt"
    # new_rdf_source_ns = "http://kg.org/rdf/"
    # shade_rdf_path = "/home/marvin/project/odibel/data_1k/sources/source.nt"

    # seed_source = "/home/marvin/project/odibel/data_1k/build/seed.nt.enriched.nt"
    # shade_seed_path = "/home/marvin/project/odibel/data_1k/sources/seed.nt"
    # new_seed_ns = "http://kg.org/seed/"

    # def shade_rdf_source(source: str, new_namespace: str) -> None:
    #     graph = Graph()
    #     graph.parse(source, format="nt")
    #     new_graph, old_to_new_uri = shade_graph_func(graph, new_namespace, get_suffix, ignored_predicates=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"])
    #     new_graph.serialize(shade_rdf_path, format="nt")
    #     json.dump(old_to_new_uri, open(shade_rdf_path.replace(".nt", ".json"), "w"))


    # def shade_seed_graph(source: str, new_namespace: str) -> None:
    #     graph = Graph()
    #     graph.parse(source, format="nt")
    #     new_graph, old_to_new_uri = shade_graph_func(graph, new_namespace, get_suffix, ignored_predicates=["http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#label"])
    #     new_graph.serialize(shade_seed_path, format="nt")
    #     json.dump(old_to_new_uri, open(shade_seed_path.replace(".nt", ".json"), "w"))
    
    # shade_rdf_source(rdf_source, new_rdf_source_ns)
    # shade_seed_graph(seed_source, new_seed_ns)
    # new_graph.serialize(shade_rdf_source, format="nt")
    # print(new_graph)
    # print(old_to_new_uri)
    
    # def shade_rdf_source(source: str, new_namespace: str) -> str:
    # shade_graph