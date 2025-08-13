from unittest import result
from dotenv import load_dotenv
import os
from numpy import sort
from pyodibel.rdf_ops.construct import DirectMappingType
import logging

def setup_logging(log_file='pyodibel.log', level=logging.INFO):
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Check if the root logger already has handlers (avoid adding multiple)
    if not root_logger.handlers or True:
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)

        # Add the handler to the root logger
        root_logger.addHandler(file_handler)
    else:
        print("Logging already setup")
        # show all handlers
        for handler in root_logger.handlers:
            print(handler)

# Call this once at the start of your application
setup_logging()

"""
Will generate a multi-source KG benchmark dataset from
Wikipedia
DBpedia
and Wikidata
"""

load_dotenv()

RAW_DIR = os.getenv("RAW_DIR")

NAMESPACE_DBOnto = "http://dbpedia.org/ontology/"
NAMESPACE_DBProp = "http://dbpedia.org/property/"
NAMESPACE_RDFS = "http://www.w3.org/2000/01/rdf-schema#"
NAMESPACE_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NAMESPACE_FOAF = "http://xmlns.com/foaf/0.1/"

DBProp_DIRECT_MAPPINGS = {
# rdfs:label
    f"{NAMESPACE_RDFS}label": DirectMappingType.LITERAL,
# foaf:name
    f"{NAMESPACE_FOAF}name": DirectMappingType.LITERAL,
# dbp:genre
    f"{NAMESPACE_DBProp}genre": DirectMappingType.LITERAL,
# dbp:writer
    f"{NAMESPACE_DBProp}writer": DirectMappingType.LITERAL,
# dbp:starring
    f"{NAMESPACE_DBProp}starring": DirectMappingType.LITERAL,

# dbp:director
    f"{NAMESPACE_DBProp}director": DirectMappingType.LITERAL,
# dbp:distributor
    f"{NAMESPACE_DBProp}distributor": DirectMappingType.LITERAL,
# dbp:runtime
    f"{NAMESPACE_DBProp}runtime": DirectMappingType.LITERAL,
# dbp:producer
    f"{NAMESPACE_DBProp}producer": DirectMappingType.LITERAL,
# dbp:budget
    f"{NAMESPACE_DBProp}budget": DirectMappingType.LITERAL,
# dbp:gross
    f"{NAMESPACE_DBProp}gross": DirectMappingType.LITERAL,
# dbp:cinematography
    f"{NAMESPACE_DBProp}cinematography": DirectMappingType.LITERAL,
# dbp:music
    f"{NAMESPACE_DBProp}music": DirectMappingType.LITERAL,
# dbp:title # TODO check if this is correct
    f"{NAMESPACE_DBProp}title": DirectMappingType.LITERAL,
# dbp:birthDate
    f"{NAMESPACE_DBProp}birthDate": DirectMappingType.LITERAL,
# dbp:deathDate
    f"{NAMESPACE_DBProp}deathDate": DirectMappingType.LITERAL,
# dbp:birthPlace
    f"{NAMESPACE_DBProp}birthPlace": DirectMappingType.LITERAL,
# dbp:deathPlace
    f"{NAMESPACE_DBProp}deathPlace": DirectMappingType.LITERAL,
# dbp:occupation
    f"{NAMESPACE_DBProp}occupation": DirectMappingType.LITERAL,
# dbp:nationality
    f"{NAMESPACE_DBProp}nationality": DirectMappingType.LITERAL,
# dbp:spouse
    f"{NAMESPACE_DBProp}spouse": DirectMappingType.LITERAL,
# dbp:child
    f"{NAMESPACE_DBProp}child": DirectMappingType.LITERAL,
# dbp:award
    f"{NAMESPACE_DBProp}award": DirectMappingType.LITERAL,

# dbp:name
    f"{NAMESPACE_DBProp}name": DirectMappingType.LITERAL,
# dbp:founded
    f"{NAMESPACE_DBProp}founded": DirectMappingType.LITERAL,
# dbp:products
    f"{NAMESPACE_DBProp}products": DirectMappingType.LITERAL,
# dbp:industry
    f"{NAMESPACE_DBProp}industry": DirectMappingType.LITERAL,
# dbp:revenue
    f"{NAMESPACE_DBProp}revenue": DirectMappingType.LITERAL,
# dbp:numEmployees
    f"{NAMESPACE_DBProp}numEmployees": DirectMappingType.LITERAL,
# dbp:headquarter
    f"{NAMESPACE_DBProp}headquarter": DirectMappingType.LITERAL,
}

DBOnto_DIRECT_MAPPINGS = {
# rdfs:label
    f"{NAMESPACE_RDFS}label": DirectMappingType.LITERAL,
# foaf:name
    f"{NAMESPACE_FOAF}name": DirectMappingType.LITERAL,

# dbo:writer
    f"{NAMESPACE_DBOnto}writer": DirectMappingType.OBJECT,
# dbo:starring
    f"{NAMESPACE_DBOnto}starring": DirectMappingType.OBJECT,
# dbo:productionCompany
    f"{NAMESPACE_DBOnto}productionCompany": DirectMappingType.OBJECT,
# dbo:director
    f"{NAMESPACE_DBOnto}director": DirectMappingType.OBJECT,
# dbo:distributor
    f"{NAMESPACE_DBOnto}distributor": DirectMappingType.OBJECT,
# dbo:runtime
    f"{NAMESPACE_DBOnto}runtime": DirectMappingType.OBJECT,
# dbo:producer
    f"{NAMESPACE_DBOnto}producer": DirectMappingType.OBJECT,
# dbo:budget
    f"{NAMESPACE_DBOnto}budget": DirectMappingType.OBJECT,
# dbo:cinematography
    f"{NAMESPACE_DBOnto}cinematography": DirectMappingType.OBJECT,
# dbo:musicComposer
    f"{NAMESPACE_DBOnto}musicComposer": DirectMappingType.OBJECT,
# dbo:title
    f"{NAMESPACE_DBOnto}title": DirectMappingType.OBJECT,
# dbo:birthDate
    f"{NAMESPACE_DBOnto}birthDate": DirectMappingType.OBJECT,
# dbo:deathDate
    f"{NAMESPACE_DBOnto}deathDate": DirectMappingType.OBJECT,
# dbo:birthPlace
    f"{NAMESPACE_DBOnto}birthPlace": DirectMappingType.OBJECT,
# dbo:deathPlace
    f"{NAMESPACE_DBOnto}deathPlace": DirectMappingType.OBJECT,
# dbo:occupation
    f"{NAMESPACE_DBOnto}occupation": DirectMappingType.OBJECT,
# dbo:nationality
    f"{NAMESPACE_DBOnto}nationality": DirectMappingType.OBJECT,
# dbo:spouse
    f"{NAMESPACE_DBOnto}spouse": DirectMappingType.OBJECT,
# dbo:child
    f"{NAMESPACE_DBOnto}child": DirectMappingType.OBJECT,
# dbo:award
    f"{NAMESPACE_DBOnto}award": DirectMappingType.OBJECT,

# dbo:foundingDate
    f"{NAMESPACE_DBOnto}foundingDate": DirectMappingType.OBJECT,
# dbo:industry
    f"{NAMESPACE_DBOnto}industry": DirectMappingType.OBJECT,
# dbo:revenue
    f"{NAMESPACE_DBOnto}revenue": DirectMappingType.OBJECT,
# dbo:numberOfEmployees
    f"{NAMESPACE_DBOnto}numberOfEmployees": DirectMappingType.OBJECT,
# dbo:headquarter
    f"{NAMESPACE_DBOnto}headquarter": DirectMappingType.OBJECT,
}

# DIR_RAW_DATA = os.getenv("DIR_RAW_DATA")
# DIR_OUTPUT = os.getenv("DIR_OUTPUT")
# DIR_SPLIT_FILES = os.getenv("DIR_SPLIT_FILES")

DIR_RAW_DATA = "/home/marvin/project/data/filehash_raw_data"
DIR_OUTPUT = "/home/marvin/project/data/inc-movie-1k"
DIR_SPLIT_FILES = "/home/marvin/project/data/acquisiton/splits1k20n"

def generate_split_files():
    # split -l 200 --numeric-suffixes=1 --suffix-length=1 --additional-suffix=.txt ../final_dbp_1k.txt split
    pass


from pyodibel.rdf_ops.filehashstore import FileHashStore2
from rdflib import Graph

def get_all_properties_from_ontology():
    graph = Graph()
    graph.parse("/home/marvin/project/data/ontology.ttl", format="turtle")
    
    results = graph.query("SELECT ?p WHERE { VALUES ?type { owl:ObjectProperty owl:DatatypeProperty } ?p a ?type }")
    equivalent_properties = graph.query("SELECT ?p WHERE { ?p0 owl:equivalentProperty ?p }")
    
    predicates = [str(r.get("p")) for r in results] + [str(r.get("p")) for r in equivalent_properties]
    predicates = list(set(predicates))
    return predicates


def test_get_all_properties_from_ontology():
    for p in get_all_properties_from_ontology():
        print(p)


def read_uri_list_file(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f.readlines()]

from pyodibel.rdf_ops.construct import construct_graph_from_root_uris
from pyodibel.rdf_ops.inference import enrich_type_information
from kgbench_extras.common.ontology import OntologyUtil

def generate_rdf(split_file_path):

    split_name = os.path.basename(split_file_path).split(".")[0]
    output_dir_tmp = os.path.join(DIR_OUTPUT, split_name, "rdf_tmp")
    os.makedirs(output_dir_tmp, exist_ok=True)



    uri_list = read_uri_list_file(split_file_path)
    print(f"Generating RDF for {split_name} with {len(uri_list)} URIs")
    construct_graph_from_root_uris(uri_list, DIR_RAW_DATA, output_dir_tmp, DBOnto_DIRECT_MAPPINGS)

    output_dir = os.path.join(DIR_OUTPUT, split_name, "rdf")
    os.makedirs(output_dir, exist_ok=True)

    ontology = OntologyUtil.load_ontology_from_file(os.getenv("ONTOLOGY_PATH"))
    for file in tqdm(os.listdir(output_dir_tmp), desc="Enriching type information"):
        graph = Graph()
        graph.parse(os.path.join(output_dir_tmp, file), format="nt")
        graph = enrich_type_information(graph, ontology)
        graph.serialize(os.path.join(output_dir, file), format="nt")

from pyodibel.rdf_ops.construct import build_recursive_json, hash_uri
import json
from tqdm import tqdm

def generate_json(split_file_path): 

    split_name = os.path.basename(split_file_path).split(".")[0]
    output_dir = os.path.join(DIR_OUTPUT, split_name, "json")
    output_dir_tmp = os.path.join(DIR_OUTPUT, split_name, "json_tmp")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_tmp, exist_ok=True)

    uri_list = read_uri_list_file(split_file_path)
    print(f"Generating JSON tmp RDF for {split_name} with {len(uri_list)} URIs")

    construct_graph_from_root_uris(
        uri_list, 
        DIR_RAW_DATA, 
        output_dir_tmp, 
        DBProp_DIRECT_MAPPINGS
    )

    tmp_store = FileHashStore2(base_dir=output_dir_tmp)
    print(f"Generating JSON for {split_name} with {len(uri_list)} URIs")
    for uri in tqdm(uri_list):
        graph = tmp_store.retrieve(uri)
        jsondata = build_recursive_json(uri, graph)
        with open(os.path.join(output_dir, hash_uri(uri)+".json"), "w") as f:
            json.dump(jsondata, f, indent=4)

import shutil
from rdflib import URIRef

def generate_text(split_file_path): 

    split_name = os.path.basename(split_file_path).split(".")[0]
    output_dir = os.path.join(DIR_OUTPUT, split_name, "text")
    os.makedirs(output_dir, exist_ok=True)

    uri_list = read_uri_list_file(split_file_path)
    print(f"Generating Text for {split_name} with {len(uri_list)} URIs")

    for root_uri in tqdm(uri_list):
        hash = hash_uri(root_uri)
        # copy file from input_dir to output_dir
        try:
            shutil.copy(os.path.join("/home/marvin/project/data/current/workdir/summaries", hash+".txt"), os.path.join(output_dir, hash+".txt"))
        except FileNotFoundError:
            print(f"File not found: {hash+'.txt'}")

def generate_metadata(split_file_path):

    split_name = os.path.basename(split_file_path).split(".")[0]
    output_dir = os.path.join(DIR_OUTPUT, split_name, "metadata")
    os.makedirs(output_dir, exist_ok=True)

    uri_list = read_uri_list_file(split_file_path)
    print(f"Generating Metadata for {split_name} with {len(uri_list)} URIs")

    shutil.copy(split_file_path, os.path.join(output_dir, "rdf_film_entities.txt"))

    # read rdf dir into a graph
    graph = Graph()
    for file in tqdm(os.listdir(os.path.join(DIR_OUTPUT, split_name, "rdf"))):
        graph.parse(os.path.join(DIR_OUTPUT, split_name, "rdf", file), format="nt")

    # get set of all entities
    entities = set()
    for s, p, o in graph:
        entities.add(str(s))
        if isinstance(o, URIRef):
            entities.add(str(o))
    
    hash_to_uri_map = {}
    for uri in entities:
        hash_to_uri_map[hash_uri(uri)] = uri

    # create a csv file with the following columns: uri, label, type
    with open(os.path.join(output_dir, "all_entities.csv"), "w") as f:
        f.write("uri,label,type\n")
        for uri in entities:
            f.write(f"{uri},,\n")

    # get json dir root uri hashes from file name
    json_dir = os.path.join(DIR_OUTPUT, split_name, "json")
    json_file_names = os.listdir(json_dir)
    json_root_uri_hashes = [os.path.basename(file).split(".")[0] for file in json_file_names]

    # write list of uris to file json entities
    with open(os.path.join(output_dir, "json_film_entities.txt"), "w") as f:
        for uri in json_root_uri_hashes:
            if uri in hash_to_uri_map:
                f.write(f"{hash_to_uri_map[uri]}\n")
            else:
                print(f"URI not found: {uri}")

    # get text dir root uri hashes from file name
    text_dir = os.path.join(DIR_OUTPUT, split_name, "text")
    text_file_names = os.listdir(text_dir)
    text_root_uri_hashes = [os.path.basename(file).split(".")[0] for file in text_file_names]

    # write list of uris to file text entities
    with open(os.path.join(output_dir, "text_film_entities.txt"), "w") as f:
        for uri in text_root_uri_hashes:
            if uri in hash_to_uri_map:
                f.write(f"{hash_to_uri_map[uri]}\n")
            else:
                print(f"URI not found: {uri}")


def generate_reference(split_file_path): pass

def process_split(split_file_path):

    print(f"Generating data for {split_file_path}")
    generate_rdf(os.path.join(DIR_SPLIT_FILES, split_file_path))
    generate_json(os.path.join(DIR_SPLIT_FILES, split_file_path))
    generate_text(os.path.join(DIR_SPLIT_FILES, split_file_path))
    generate_metadata(os.path.join(DIR_SPLIT_FILES, split_file_path))
    print(f"Done generating data for {split_file_path}")
    print("-"*100)

def generate_overlaps(split_file_paths, overlap_size, overlap_count):
    """
    split_file_paths: list of split file paths
    overlap_size: number of splits to overlap
    overlap_count: number of generated overlaps
    TODO implement
    """

    raise NotImplementedError("Not implemented")
    overlaps = []
    for i in range(overlap_count):
        overlaps.append(split_file_paths[i*overlap_size:(i+1)*overlap_size])
    return overlaps

def aggregate(splits, subdir, name=None):
    """
    splits: list of split file paths
    subdir: subdirectory of output dir
    name: name of the aggregate
    """

    if name is None:
        name = subdir

    output_dir = os.path.join(DIR_OUTPUT, "aggregated", name)
    os.makedirs(output_dir, exist_ok=True)

    # entities
    entities = set()

    for split in splits:
        realsub = "root" if subdir == "rdf" else subdir
        split_dir = os.path.join(DIR_OUTPUT, "splited", split)
        split_metadata_dir = os.path.join(split_dir, "metadata")
        split_subdir = os.path.join(split_dir, subdir)
        with open(os.path.join(split_metadata_dir, f"{realsub}_film_entities.txt"), "r") as f:
            for line in f:
                entities.add(line.strip())

        for file in os.listdir(split_subdir):
            shutil.copy(os.path.join(split_subdir, file), output_dir)

    # aggregate metadata
    metadata_dir = os.path.join(os.path.join(DIR_OUTPUT, "aggregated"), f"{name}_film_entities.txt")
    with open(metadata_dir, "w") as f:
        for entity in entities:
            f.write(f"{entity}\n")

def generate_inc_movie_kgb():

    split_file_paths = sorted(os.listdir(DIR_SPLIT_FILES))

    # generate all data for each split
    for split_file_path in split_file_paths:
        process_split(split_file_path)

    # overlaps = generate_overlaps(split_file_paths, overlap_size=5, overlap_count=3)

    # overlaps = [
    #     [1,2,3,4,5],
    #     [5,6,7,8,9,10],
    #     [5,10,11,12,13,14,15],
    #     [5,10,15,16,17,18,19,20],
    # ]
    # overlaps = [[ f"split{int(i):02d}" for i in o] for o in overlaps]

    # # aggregate with overlap
    # for idx, overlap in enumerate(overlaps):
    #     print(f"Aggregating with overlap {overlap}")
    #     aggregate(overlap, "rdf", f"rdf_{idx}") # done for every overlap
    #     aggregate(overlap, "json", f"json_{idx}")
    #     aggregate(overlap, "text", f"text_{idx}")

    # aggregate without overlap
    # aggregate([s.split(".")[0] for s in split_file_paths], "rdf", "reference")

    """
    output/
        split1/
            entities.csv # uri, label, type
            rdf/
                film1.nt
            json/
                film1.json
            text/
                film1.txt
            reference/
                entities.csv # uri, label, type 
                film1.nt # mapped to dbpedia if not possible wikidata
        splitN/
            entities.csv
            rdf/
            json/
            text/
            reference/
    """

    # 0 [~] ingest data into raw store (allows storing both wikdiata and dbpedia)
    # 1. split film entities | 
    # 2. generate rdf | filter | 
    # 3. generate reference | map to dbpedia if not possible wikidata (person company) |

    # all splits with
    #  - rdf
    #  - json
    #  - text
    #  - root films
    #  - full reference graph
    #  - related persons / companies
    pass

def test_generate_inc_movie_kgb():
    generate_inc_movie_kgb()

def evaluate_inc_movie_kgb():
    pass

def test_evaluate_inc_movie_kgb():
    evaluate_inc_movie_kgb()

def test_full_reference_graph(): 
    pass