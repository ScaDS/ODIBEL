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

from functools import lru_cache
from rdflib import URIRef, Literal, Graph

# --- lightweight caches (tune maxsize to your corpus size) ---
@lru_cache(maxsize=200_000)
def _retrieve_cached(uri: str, store_id: int) -> Graph:
    # store_id prevents cross-store cache pollution if multiple stores are used
    return _STORE_REGISTRY[store_id].retrieve(uri)

@lru_cache(maxsize=200_000)
def _label_cached(uri: str, store_id: int) -> str:
    g = _retrieve_cached(uri, store_id)
    return get_label(g, uri)

# Registry to let caches reference the live store by id
_STORE_REGISTRY = {}

def register_store_for_cache(store):
    sid = id(store)
    _STORE_REGISTRY[sid] = store
    return sid

def direct_map_triple_fast(entity_uri: str,
                      triple: "_TripleType",
                      store: "FileHashStore",
                      output_store: "FileHashStore",
                      dict_map: "Dict[str, DirectMappingType]",
                      depth: int = 0,
                      visited: "Optional[set]" = None,
                      *,
                      # internal optimization flags
                      _store_id: "Optional[int]" = None,
                      _writes: "Optional[list]" = None  # collect (uri, Graph) to write later
                      ) -> "Optional[_TripleType]":
    """Map a triple to a new triple (optimized: cached retrieve/label, no set copying, deferred writes)."""

    # One-time registration for caches
    if _store_id is None:
        _store_id = register_store_for_cache(store)

    if visited is None:
        visited = set()

    if depth > 5 or entity_uri in visited:
        return None

    # bind locals to avoid global lookups in tight loops
    s0, p1, o2 = triple[0], triple[1], triple[2]
    s0_str = str(s0)
    p1_str = str(p1)

    # subject must match entity
    if s0_str != entity_uri:
        return None

    # faster membership check with a local
    in_map = p1_str in dict_map


    if in_map:
        dtype = dict_map[p1_str]

        # Only URIRef objects need dereferencing
        if isinstance(o2, URIRef):
            o2_str = str(o2)

            if dtype == DirectMappingType.LITERAL:
                # Cached dereference + label
                label = _label_cached(o2_str, _store_id)
                if label == "":
                    return None
                return (s0, p1, Literal(label))

            elif dtype == DirectMappingType.OBJECT:
                # Recursively map the object node
                visited.add(entity_uri)         # push
                obj_graph = _retrieve_cached(o2_str, _store_id)
                new_obj_graph = Graph()

                # bind callables
                _add = new_obj_graph.add
                _map = direct_map_triple_fast

                for obj_triple in obj_graph:
                    if str(obj_triple[1]).endswith("genre"):
                        continue # TODO this is a hack to remove genres from the sub entities
                    mapped = _map(o2_str, obj_triple, store, output_store, dict_map,
                                  depth + 1, visited, _store_id=_store_id, _writes=_writes)
                    if mapped is not None:
                        _add(mapped)

                visited.remove(entity_uri)      # pop

                if len(new_obj_graph) > 0:
                    # Defer write if _writes is provided (preferred), else write now for backward-compat
                    if _writes is not None:
                        _writes.append((o2_str, new_obj_graph))
                    else:
                        output_store.store(o2_str, new_obj_graph)
                    return triple
                else:
                    return None

            else:
                # dtype matches but object isn't to be dereferenced -> keep triple
                return triple

        else:
            # dtype matches but object isn't URIRef -> keep triple
            if o2 is not None and str(o2).strip() != "":
                return triple
            else:
                return None

    # Not in dict_map: allow only whitelisted rdf:type
    if p1_str == f"{NAMESPACE_RDF}type":
        o2_str = str(o2)
        if o2_str in (f"{NAMESPACE_DBOnto}Film",
                      f"{NAMESPACE_DBOnto}Person",
                      f"{NAMESPACE_DBOnto}Organisation"):
            return triple
        return None

    return None

def direct_map_triple_slow(entity_uri: str, triple: _TripleType, store: FileHashStore, output_store: FileHashStore, dict_map: Dict[str, DirectMappingType], depth: int = 0, visited: Optional[set] = None) -> Optional[_TripleType]:
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
                mapped_triple = direct_map_triple_slow(str(triple[2]), object_triple, store, output_store, dict_map, depth + 1, visited.copy())
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

from concurrent.futures import ThreadPoolExecutor, as_completed

def construct_graph_from_root_uris(uris: list[str], direct_mappings: Dict[str, DirectMappingType], input_store: FileHashStore2, output_store: FileHashStore2) -> None:
    """Construct a graph from a list of URIs, predicates, types, and namespaces."""


    def build_graph(uri):
        writes = []  # collect child graphs to store after mapping
        g = input_store.retrieve(uri)  # top-level retrieve (not cached on purpose; you can also cache this)
        new_g = Graph()
        _add = new_g.add
        _map = direct_map_triple_fast

        for t in g:
            mt: Optional[_TripleType]  = _map(uri, t, input_store, output_store, direct_mappings, _writes=writes)
            if mt is not None and mt[2] is not None and str(mt[2]).strip() != "":
                _add(mt)

        return uri, new_g, writes

    MAX_WORKERS = 16  # tune 8–32 for I/O-bound backends
    empty_graphs = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for uri, new_g, writes in tqdm(ex.map(build_graph, uris, chunksize=32),
                                    total=len(uris),
                                    desc="Constructing graph from root URIs"):
            if len(new_g) > 0:
                # perform deferred writes on the main thread
                for child_uri, child_graph in writes:
                    output_store.store(child_uri, child_graph)
                output_store.store(uri, new_g)
            else:
                empty_graphs += 1

    print(f"Empty graphs: {empty_graphs}")

    # def build_graph(uri):
    #     graph = input_store.retrieve(uri)
    #     new_graph = Graph()
    #     # Micro-opts: bind locals
    #     _map = direct_map_triple
    #     _add = new_graph.add
    #     for t in graph:
    #         mt = _map(uri, t, input_store, output_store, direct_mappings)
    #         if mt is not None:
    #             _add(mt)
    #     # Return instead of writing here to avoid concurrent store contention
    #     return uri, new_graph

    # # Tune: for I/O, a modest cap often wins vs. unlimited
    # MAX_WORKERS = 16  # try 8–32; measure
    # empty_graphs = 0

    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    #     results = ex.map(build_graph, uris, chunksize=32)  # chunksize reduces scheduling overhead
    #     for uri, new_graph in tqdm(results, total=len(uris), desc="Constructing graph from root URIs"):
    #         if len(new_graph) > 0:
    #             output_store.store(uri, new_graph)  # single-threaded writes
    #         else:
    #             empty_graphs += 1

    # print(f"Empty graphs: {empty_graphs}")
    # def process_uri(uri):
    #     graph = input_store.retrieve(uri)
    #     new_graph = Graph()

    #     for triple in graph:
    #         mapped_triple = direct_map_triple(uri, triple, input_store, output_store, direct_mappings)
    #         if mapped_triple is not None:
    #             new_graph.add(mapped_triple)

    #     if len(new_graph) > 0:
    #         output_store.store(uri, new_graph)
    #         return 0  # not empty
    #     else:
    #         return 1  # empty

    # empty_graphs = 0
    # with ThreadPoolExecutor(max_workers=16) as ex:  # or set an int, e.g. max_workers=8
    #     results = ex.map(process_uri, uris, chunksize=32)  # tweak chunksize as needed
    #     empty_graphs = sum(tqdm(results, total=len(uris), desc="Constructing graph from root URIs"))
    #     # futures = [ex.submit(process_uri, uri) for uri in uris]
    #     # for fut in tqdm(as_completed(futures), total=len(futures), desc="Constructing graph from root URIs"):
    #     #     empty_graphs += fut.result()

    # print(f"Empty graphs: {empty_graphs}")

    # empty_graphs = 0

    # for uri in tqdm(uris, desc="Constructing graph from root URIs"):
    #     graph = input_store.retrieve(uri)
    #     new_graph = Graph()
        
    #     for triple in graph:
    #         mapped_triple = direct_map_triple(uri, triple, input_store, output_store, direct_mappings)
    #         if mapped_triple is not None:
    #             new_graph.add(mapped_triple)
        
    #     if len(new_graph) > 0:
    #         output_store.store(uri, new_graph)
    #     else:
    #         empty_graphs += 1
    
    # print(f"Empty graphs: {empty_graphs}")



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