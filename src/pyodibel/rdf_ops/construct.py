import json
import os
import shutil
# from pyodibel.rdf_ops.construct import read_uris_from_file
from pyodibel.rdf_ops.filehashstore import FileHashStore, hash_uri, FileHashStore2
from dataclasses import dataclass
from rdflib import Graph, URIRef, Literal
from rdflib.graph import _TripleType
from typing import Dict, List, Optional

# enum for Object or Literal
from enum import Enum

from tqdm import tqdm
from pyodibel.rdf_ops.utils import build_recursive_json

class DirectMappingType(Enum):
    OBJECT = "object"
    LITERAL = "literal"


NAMESPACE_DBOnto = "http://dbpedia.org/ontology/"
NAMESPACE_DBProp = "http://dbpedia.org/property/"
NAMESPACE_RDFS = "http://www.w3.org/2000/01/rdf-schema#"
NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NAMESPACE_FOAF = "http://xmlns.com/foaf/0.1/"

def read_uris_from_file(file_path: str) -> list[str]:
    """Read URIs from a file."""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def get_label(graph: Graph, uri: str) -> str:
    """Get the label of a URI."""
    # print(f"Getting label for {uri}")
    for triple in graph:
        if str(triple[0]) == uri and str(triple[1]) == "http://www.w3.org/2000/01/rdf-schema#label":
            if isinstance(triple[2], Literal):
                return str(triple[2])
    return ""

def direct_map_triple(entity_uri: str, triple: _TripleType, store: FileHashStore, output_store: FileHashStore, dict_map: Dict[str, DirectMappingType], depth: int = 0, visited: Optional[set] = None) -> Optional[_TripleType]:
    """Map a triple to a new triple."""
    # Initialize visited set for cycle detection
    if visited is None:
        visited = set()
    
    # Prevent infinite recursion by limiting depth and detecting cycles
    if depth > 5 or entity_uri in visited:
        return None
    
    # Add current entity to visited set
    visited.add(entity_uri)
    
    # print(f"Mapping triple: {triple}")
    if str(triple[0]) != entity_uri:
        return None
    if str(triple[1]) in dict_map:
        # print(f"Mapping triple: {triple}")
        direct_mapping_type = dict_map[str(triple[1])]
        if direct_mapping_type == DirectMappingType.LITERAL and isinstance(triple[2], URIRef):
            object_graph = store.retrieve(str(triple[2]))            
            label = get_label(object_graph, str(triple[2]))
            return (triple[0], triple[1], Literal(label))
        elif direct_mapping_type == DirectMappingType.OBJECT and isinstance(triple[2], URIRef):
            object_graph = store.retrieve(str(triple[2]))     
            new_object_graph = Graph()       
            for object_triple in object_graph:
                mapped_triple = direct_map_triple(str(triple[2]), object_triple, store, output_store, dict_map, depth + 1, visited.copy())
                if mapped_triple is not None:
                    new_object_graph.add(mapped_triple)
            if len(new_object_graph) > 0:
                output_store.store(str(triple[2]), new_object_graph)
                return triple
            else:
                return None
        else:
            return triple
    elif str(triple[1]) == f"{NAMESPACE_RDF}type":
        if str(triple[2]) in [f"{NAMESPACE_DBOnto}Film", f"{NAMESPACE_DBOnto}Person", f"{NAMESPACE_DBOnto}Organisation"]:
            return triple
        else:
            return None
    return None

def construct_graph_from_root_uris(uris: list[str], base_dir: str, output_dir: str, direct_mappings: Dict[str, DirectMappingType]) -> None:
    """Construct a graph from a list of URIs, predicates, types, and namespaces."""
    input_store = FileHashStore2(base_dir=base_dir)
    output_store = FileHashStore2(base_dir=output_dir)

    for uri in tqdm(uris):
        graph = input_store.retrieve(uri)
        new_graph = Graph()
        
        for triple in graph:
            mapped_triple = direct_map_triple(uri, triple, input_store, output_store, direct_mappings)
            if mapped_triple is not None:
                new_graph.add(mapped_triple)
        
        if len(new_graph) > 0:
            output_store.store(uri, new_graph)



# file_10k = "/home/marvin/project/data/acquisiton/final_dbp_10k.txt"

SPLIT_BASE_DIR = "/home/marvin/project/data/acquisiton/splits1k/"

if __name__ == "__main__":

    split1 = os.path.join(SPLIT_BASE_DIR, "split1.txt")
    split2 = os.path.join(SPLIT_BASE_DIR, "split2.txt")
    split3 = os.path.join(SPLIT_BASE_DIR, "split3.txt")
    split4 = os.path.join(SPLIT_BASE_DIR, "split4.txt")
    split5 = os.path.join(SPLIT_BASE_DIR, "split5.txt")

    def generate_seed(split_files: list[str], output_dir: str, direct_mappings: Dict[str, DirectMappingType]):

        os.makedirs(output_dir, exist_ok=True)

        uris = []
        for split_file in split_files:
            uris.extend(read_uris_from_file(split_file))

        construct_graph_from_root_uris(
            uris, 
            "/home/marvin/project/data/current/workdir/raw_data_flat/", 
            output_dir, 
            direct_mappings
        )

    def gerenrate_rdf_source(split_files: list[str], output_dir: str, direct_mappings: Dict[str, DirectMappingType]):
        # TODO remove triples
        generate_seed(split_files, output_dir, direct_mappings)

    def gnerate_json_data(input_dir: str, output_dir: str, root_uris: list[str]):
        input_store = FileHashStore(base_dir=input_dir)

        for root_uri in root_uris:
            graph = input_store.retrieve(root_uri)
            # TODO to json


    def generate_json_source(split_files: list[str], output_dir: str, direct_mappings: Dict[str, DirectMappingType]):
        # TODO remove triples
        generate_seed(split_files, output_dir, direct_mappings)

        root_uris = []
        for split_file in split_files:
            root_uris.extend(read_uris_from_file(split_file))

        tmp_store = FileHashStore2(base_dir=output_dir)
        for uri in tqdm(root_uris):
            graph = tmp_store.retrieve(uri)
            jsondata = build_recursive_json(uri, graph)
            with open(os.path.join(output_dir, hash_uri(uri)+".json"), "w") as f:
                json.dump(jsondata, f, indent=4)



    def generate_text_source(split_files: list[str], output_dir: str):

        os.makedirs(output_dir, exist_ok=True)

        root_uris = []
        for split_file in split_files:
            root_uris.extend(read_uris_from_file(split_file))

        for root_uri in tqdm(root_uris):
            hash = hash_uri(root_uri)
            # copy file from input_dir to output_dir
            try:
                shutil.copy(os.path.join("/home/marvin/project/data/current/workdir/summaries", hash+".txt"), os.path.join(output_dir, hash+".txt"))
            except FileNotFoundError:
                print(f"File not found: {hash+'.txt'}")

    # generate_seed([split1], "/home/marvin/project/odibel/data_seed_graph/", DBOnto_DIRECT_MAPPINGS)
    # gerenrate_rdf_source([split1,split2,split5], "/home/marvin/project/odibel/data_rdf_source/", DBOnto_DIRECT_MAPPINGS)
    # generate_json_source([split1,split2,split5], "/home/marvin/project/odibel/data_json_source/", DBProp_DIRECT_MAPPINGS)
    # generate_text_source([split1,split4,split5], "/home/marvin/project/odibel/data_text_source/")
    # generate_seed([split1,split2,split3,split4,split5], "/home/marvin/project/odibel/data_full_graph/", DBOnto_DIRECT_MAPPINGS)

    # generate_seed([split1], "/home/marvin/project/odibel/data_split1_graph/", DBOnto_DIRECT_MAPPINGS)
    # generate_seed([split2], "/home/marvin/project/odibel/data_split2_graph/", DBOnto_DIRECT_MAPPINGS)
    # generate_seed([split3], "/home/marvin/project/odibel/data_split3_graph/", DBOnto_DIRECT_MAPPINGS)
    # generate_seed([split4], "/home/marvin/project/odibel/data_split4_graph/", DBOnto_DIRECT_MAPPINGS)
    # generate_seed([split5], "/home/marvin/project/odibel/data_split5_graph/", DBOnto_DIRECT_MAPPINGS)