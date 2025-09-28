from unittest import result
from dotenv import load_dotenv
import os
from numpy import sort
from pyodibel.rdf_ops.construct import DirectMappingType
import logging

import shutil
from rdflib import URIRef, RDF, RDFS, OWL, SKOS

from pyodibel.datasets.mp_mf.multipart_multisource import Dataset, EntitiesRow, MatchesRow, KGBundle, LinksRow, SourceBundle, SourceType, load_dataset
from pyodibel.datasets.mp_mf.overlap_util import build_exact_subsets, validate_overlaps

from pathlib import Path

load_dotenv()

DATASET_SELECT=os.getenv("DATASET_SELECT", "not set")

if DATASET_SELECT == "small":
    BUNDLE_DIR = Path("/home/marvin/project/data/final/film_100")
    ENTITY_LIST_PATH = Path("/home/marvin/project/data/acq/selection_100_dbpedia_film")
    ACQ_DIR = Path("/home/marvin/project/data/work/")
    NUM_SUBSETS = 4
    OVERLAP_RATIO = 0.04
    SUBSET_SIZE = 25
elif DATASET_SELECT == "medium":
    BUNDLE_DIR = Path("/home/marvin/project/data/final/film_1k")
    ENTITY_LIST_PATH = Path("/home/marvin/project/data/acq/selection_1k_dbpedia_film")
    ACQ_DIR = Path("/home/marvin/project/data/work/")
    NUM_SUBSETS = 4
    OVERLAP_RATIO = 0.04
    SUBSET_SIZE = 250
elif DATASET_SELECT == "large":
    BUNDLE_DIR = Path("/home/marvin/project/data/final/film_10k")
    ENTITY_LIST_PATH = Path("/home/marvin/project/data/acq/selection_10k_dbpedia_film")
    ACQ_DIR = Path("/home/marvin/project/data/work/")
    NUM_SUBSETS = 4
    OVERLAP_RATIO = 0.04
    SUBSET_SIZE = 2500
else:
    raise ValueError(f"Invalid dataset select: {DATASET_SELECT}")

opt_ontology_path = os.getenv("ONTOLOGY_PATH")
if opt_ontology_path is None:
    raise ValueError("ONTOLOGY_PATH is not set")
ontology_path: Path = Path(opt_ontology_path)

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
    # f"{NAMESPACE_DBProp}title": DirectMappingType.LITERAL,
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
    # f"{NAMESPACE_DBProp}award": DirectMappingType.LITERAL,

# dbp:name
    # f"{NAMESPACE_DBProp}name": DirectMappingType.LITERAL,
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
    # f"{NAMESPACE_FOAF}name": DirectMappingType.LITERAL,

# === Film ===

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
    f"{NAMESPACE_DBOnto}runtime": DirectMappingType.LITERAL,
# dbo:producer
    f"{NAMESPACE_DBOnto}producer": DirectMappingType.OBJECT,
# dbo:budget
    f"{NAMESPACE_DBOnto}budget": DirectMappingType.LITERAL,
# dbo:gross
    f"{NAMESPACE_DBOnto}gross": DirectMappingType.LITERAL,
    f"{NAMESPACE_DBOnto}genre": DirectMappingType.LITERAL,
# # dbp:genre removed because of bad quality (mostly music genres...)
# f"{NAMESPACE_DBProp}genre": DirectMappingType.LITERAL,
# dbo:cinematography
    f"{NAMESPACE_DBOnto}cinematography": DirectMappingType.OBJECT,
# dbo:musicComposer
    f"{NAMESPACE_DBOnto}musicComposer": DirectMappingType.OBJECT,

# === Person ===

# dbo:title
    # f"{NAMESPACE_DBOnto}title": DirectMappingType.OBJECT,
# dbo:birthDate
    f"{NAMESPACE_DBOnto}birthDate": DirectMappingType.LITERAL,
# dbo:deathDate
    f"{NAMESPACE_DBOnto}deathDate": DirectMappingType.LITERAL,
# dbo:birthPlace
    f"{NAMESPACE_DBOnto}birthPlace": DirectMappingType.LITERAL,
# dbo:deathPlace
    f"{NAMESPACE_DBOnto}deathPlace": DirectMappingType.LITERAL,
# dbo:occupation
    f"{NAMESPACE_DBOnto}occupation": DirectMappingType.LITERAL,
# dbo:nationality
    f"{NAMESPACE_DBOnto}nationality": DirectMappingType.LITERAL,
# dbo:spouse
    # f"{NAMESPACE_DBOnto}spouse": DirectMappingType.OBJECT,
# dbo:child
    # f"{NAMESPACE_DBOnto}child": DirectMappingType.OBJECT,
# dbo:award
    # f"{NAMESPACE_DBOnto}award": DirectMappingType.LITERAL, 

# === Company ===

# dbo:foundingDate
    f"{NAMESPACE_DBOnto}foundingDate": DirectMappingType.LITERAL,
# dbo:industry
    f"{NAMESPACE_DBOnto}industry": DirectMappingType.LITERAL,
# dbo:revenue
    f"{NAMESPACE_DBOnto}revenue": DirectMappingType.LITERAL,
# dbo:numberOfEmployees
    f"{NAMESPACE_DBOnto}numberOfEmployees": DirectMappingType.LITERAL,
# dbo:headquarter
    f"{NAMESPACE_DBOnto}headquarter": DirectMappingType.LITERAL,
}

# DIR_RAW_DATA = os.getenv("DIR_RAW_DATA")
# DIR_OUTPUT = os.getenv("DIR_OUTPUT")
# DIR_SPLIT_FILES = os.getenv("DIR_SPLIT_FILES")

# DIR_RAW_DATA = ACQ_DIR / "reference"
# DIR_OUTPUT = "/home/marvin/project/data/inc-movie-1k"
# DIR_SPLIT_FILES = "/home/marvin/project/data/acquisiton/splits1k20n"

def generate_split_files():
    # split -l 200 --numeric-suffixes=1 --suffix-length=1 --additional-suffix=.txt ../final_dbp_1k.txt split
    pass


from pyodibel.rdf_ops.filehashstore import FileHashStore2
from rdflib import Graph

def get_all_properties_from_ontology():
    graph = Graph()
    graph.parse(ontology_path, format="turtle")
    
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

# def generate_rdf(split_file_path):

#     split_name = os.path.basename(split_file_path).split(".")[0]
#     output_dir_tmp = os.path.join(DIR_OUTPUT, split_name, "rdf_tmp")
#     os.makedirs(output_dir_tmp, exist_ok=True)



#     uri_list = read_uri_list_file(split_file_path)
#     print(f"Generating RDF for {split_name} with {len(uri_list)} URIs")
#     construct_graph_from_root_uris(uri_list, DIR_RAW_DATA, output_dir_tmp, DBOnto_DIRECT_MAPPINGS)

#     output_dir = os.path.join(DIR_OUTPUT, split_name, "rdf")
#     os.makedirs(output_dir, exist_ok=True)

#     ontology = OntologyUtil.load_ontology_from_file(os.getenv("ONTOLOGY_PATH"))
#     for file in tqdm(os.listdir(output_dir_tmp), desc="Enriching type information"):
#         graph = Graph()
#         graph.parse(os.path.join(output_dir_tmp, file), format="nt")
#         graph = enrich_type_information(graph, ontology)
#         graph.serialize(os.path.join(output_dir, file), format="nt")

import tempfile

from pyodibel.rdf_ops.replacer import replace_to_namespace, __load_match_clusters_from_Ontology, replace_namespace, replace_with_func_on_namespace

import hashlib
def generat_hashed_shade(uri: str) -> str:
    return "http://kg.org/resource/" + hashlib.md5(uri.encode()).hexdigest()

    
def copy_label_to_skos_alt_label(graph: Graph):
    for s, _, l in graph.triples((None, RDFS.label, None)):
        graph.add((s, SKOS.altLabel, l))
    return graph

def generate_rdf(entity_list, input_store: FileHashStore2, output_dir, mappings, ns_mapping: dict[str, str] = {}):

    tempdir = tempfile.mkdtemp()
    output_store = FileHashStore2(base_dir=tempdir)
    
    # print(tempdir)

    # for mapping in mappings.items():
    #     print(mapping)

    ontology = OntologyUtil.load_ontology_from_file(ontology_path)

    # for prop in ontology.properties:
    #     print(prop.uri)

    clusters = __load_match_clusters_from_Ontology(ontology)

    # for ck, cv in clusters.clusters.items():
    #     print(ck, cv)


    construct_graph_from_root_uris(entity_list, mappings, input_store, output_store)

    for file in tqdm(os.listdir(tempdir), desc="Postprocessing"):
        graph = Graph()
        graph.parse(os.path.join(tempdir, file), format="nt")

        graph = replace_to_namespace(graph, clusters, "http://kg.org/ontology/")
        graph = replace_with_func_on_namespace(graph, generat_hashed_shade, "http://dbpedia.org/resource/")

        for old_namespace, new_namespace in ns_mapping.items():
            graph = replace_namespace(graph, old_namespace, new_namespace)
        
        graph = enrich_type_information(graph, ontology)
        graph = copy_label_to_skos_alt_label(graph) # skos:altLabel for provenance can be removed for comparison
        graph.serialize(os.path.join(output_dir, file), format="nt")
    
    shutil.rmtree(tempdir)

from pyodibel.rdf_ops.construct import build_recursive_json, hash_uri
import json
from tqdm import tqdm

# def generate_json(split_file_path): 

#     split_name = os.path.basename(split_file_path).split(".")[0]
#     output_dir = os.path.join(DIR_OUTPUT, split_name, "json")
#     output_dir_tmp = os.path.join(DIR_OUTPUT, split_name, "json_tmp")
#     os.makedirs(output_dir, exist_ok=True)
#     os.makedirs(output_dir_tmp, exist_ok=True)

#     uri_list = read_uri_list_file(split_file_path)
#     print(f"Generating JSON tmp RDF for {split_name} with {len(uri_list)} URIs")

#     construct_graph_from_root_uris(
#         uri_list, 
#         DIR_RAW_DATA, 
#         output_dir_tmp, 
#         DBProp_DIRECT_MAPPINGS
#     )

#     tmp_store = FileHashStore2(base_dir=output_dir_tmp)
#     print(f"Generating JSON for {split_name} with {len(uri_list)} URIs")
#     for uri in tqdm(uri_list):
#         graph = tmp_store.retrieve(uri)
#         jsondata = build_recursive_json(uri, graph)
#         with open(os.path.join(output_dir, hash_uri(uri)+".json"), "w") as f:
#             json.dump(jsondata, f, indent=4)



# ================================================


def generate_overlap_xyz(entity_list_path, num_subsets, overlap_ratio, subset_size):
    with open(entity_list_path, "r") as f:
        entity_list = [line.strip() for line in f]

    subsets = build_exact_subsets(entity_list, num_subsets, overlap_ratio, subset_size)
    return subsets

# ================================================

def bundle_text_source(bundle: SourceBundle, entity_selection, entity_acq_dir):

    output_dir = bundle.data.dir.as_posix()
    missing_files = []
    empty_files = []
    verfied_uris = []
    verfied_labels = []


    for root_uri in tqdm(entity_selection):
        hash = hash_uri(root_uri)
        # copy file from input_dir to output_dir
        try:
            # check input file size > 0
            if os.path.getsize(os.path.join(entity_acq_dir, "text",hash+".txt")) == 0:
                empty_files.append(hash)
                continue
            input_file = os.path.join(entity_acq_dir, "text",hash+".txt")
            output_file = os.path.join(output_dir, hash+".txt")
            shutil.copy(input_file, output_file)
            verfied_uris.append((root_uri, hash))
            # verfied_labels.append((root_uri, label))
        except FileNotFoundError:
            missing_files.append(hash)

    if len(missing_files) > 0:
        print(f"Missing text files: {len(missing_files)}")
    if len(empty_files) > 0:
        print(f"Empty text files: {len(empty_files)}")
    
    bundle.meta.set_entities([EntitiesRow(entity_id=uri, entity_label=uri, entity_type="http://kg.org/ontology/Film", dataset="dataset") for uri, _ in verfied_uris])
    bundle.meta.set_links([LinksRow(doc_id=hash+".txt", entity_id=uri, entity_type="http://kg.org/ontology/Film", dataset="dataset") for uri, hash in verfied_uris])


# def copy_and_shade_rdf_file(input_file, output_file):
#     shutil.copy(input_file, output_file)
    # TODO implement shade
    # TODO cleanup
    #    infere missing types 
    #    remove low covered entities

def bundle_rdf_source(bundle, entity_selection, input_store: FileHashStore2, split_id):
    # missing_files = []
    # empty_files = []
    # verfied_uris = []
    
    generate_rdf(
        entity_selection, 
        input_store, 
        bundle.data.dir.as_posix(), 
        DBOnto_DIRECT_MAPPINGS,
        ns_mapping={
            "http://kg.org/resource/": f"http://kg.org/rdf/{split_id}/resource/",
            "http://kg.org/ontology/": f"http://kg.org/rdf/{split_id}/ontology/"
        }
    )
    
    graph = Graph()
    for file in os.listdir(bundle.data.dir):
        if file.endswith(".nt"):
            graph.parse(os.path.join(bundle.data.dir, file), format="nt")

    graph.serialize(bundle.root / "data.nt", format="nt")

    entities_with_types = {}
    for s, _, t in graph.triples((None, RDF.type, None)):
        entities_with_types[str(s)] = str(t)
    
    # bundle.meta.set_entities([EntitiesRow(entity_id=uri, entity_label=uri, entity_type=entities_with_types[uri], dataset="dataset") for uri in entities_with_types])

def bundle_json_source(bundle, entity_selection, entity_acq_dir):
    missing_files = []
    empty_files = []
    verfied_uris = []
    
    for root_uri in tqdm(entity_selection):
        hash = hash_uri(root_uri)
        input_file = os.path.join(entity_acq_dir, "json", hash+".json")
        output_file = os.path.join(bundle.data.dir.as_posix(), hash+".json")
        if not os.path.exists(input_file):
            missing_files.append(hash)
            continue
        if os.path.getsize(input_file) == 2: # can conain emtpy {}
            empty_files.append(hash)
            continue
        shutil.copy(input_file, output_file)
        verfied_uris.append((root_uri, hash))

    if len(missing_files) > 0:
        print(f"Missing json files: {len(missing_files)}")
    if len(empty_files) > 0:
        print(f"Empty json files: {len(empty_files)}")

    # TODO use from seed split instead
    # bundle.meta.set_entities([EntitiesRow(entity_id=uri, entity_label=uri, entity_type="http://kg.org/ontology/Film", dataset="dataset") for uri, _ in verfied_uris])
    bundle.meta.set_links([LinksRow(doc_id=hash+".json", entity_id=uri, entity_type="http://kg.org/ontology/Film", dataset="dataset") for uri, hash in verfied_uris])

def bundle_reference(bundle: KGBundle, entity_selection, input_store: FileHashStore2, split_id):
    # missing_files = []
    # empty_files = []
    # verfied_uris = []

    combined_mappings = DBOnto_DIRECT_MAPPINGS.copy()
    # combined_mappings.update(DBProp_DIRECT_MAPPINGS)

    generate_rdf(entity_selection, input_store, bundle.data.dir.as_posix(), combined_mappings, ns_mapping={
        "http://dbpedia.org/property/": f"http://kg.org/ontology/",
    })
    
    graph = Graph()
    for file in os.listdir(bundle.data.dir):
        if file.endswith(".nt"):
            graph.parse(os.path.join(bundle.data.dir, file), format="nt")

    

    graph.serialize(bundle.root / "data.nt", format="nt")
    if split_id == 0:
        graph.serialize(bundle.root / "data_agg.nt", format="nt")
    else:
        # agg with previous split
        previouse_reference_data = bundle.root / ".." / ".." / ".." / f"split_{split_id-1}" / "kg/reference/data_agg.nt"
        graph.parse(previouse_reference_data, format="nt")
        graph.serialize(bundle.root / "data_agg.nt", format="nt")

    entities_with_types = {}
    for s, _, t in graph.triples((None, RDF.type, None)):
        entities_with_types[str(s)] = str(t)

    entities_with_labels = {}
    for s, _, l in graph.triples((None, RDFS.label, None)):
        entities_with_labels[str(s)] = str(l)
    
    bundle.meta.set_entities([EntitiesRow(entity_id=uri, entity_label=entities_with_labels[uri], entity_type=entities_with_types.get(uri, OWL.Thing), dataset="dataset") for uri in entities_with_labels])


def bundle_seed(bundle: KGBundle, entity_selection, input_store):

    # missing_files = []
    # empty_files = []
    # verfied_uris = []
    
    generate_rdf(entity_selection, input_store, bundle.data.dir.as_posix(), DBOnto_DIRECT_MAPPINGS)

    graph = Graph()
    for file in os.listdir(bundle.data.dir):
        if file.endswith(".nt"):
            graph.parse(os.path.join(bundle.data.dir, file), format="nt")
    
    graph.serialize(bundle.root / "data.nt", format="nt")

    entities_with_types = {}
    for s, _, t in graph.triples((None, RDF.type, None)):
        entities_with_types[str(s)] = str(t)

    entities_with_labels = {}
    for s, _, l in graph.triples((None, RDFS.label, None)):
        entities_with_labels[str(s)] = str(l)
    
    bundle.meta.set_entities([EntitiesRow(entity_id=uri, entity_label=entities_with_labels[uri], entity_type=entities_with_types.get(uri, OWL.Thing), dataset="dataset") for uri in entities_with_labels])

# ================================================

def generate_split_bundles(subset, idx, ds, entity_acq_dir):
    split = ds.splits[f"split_{idx}"]
    split.set_empty_reference()
    split.set_empty_seed()

    entity_selection = subset
    split.set_index([EntitiesRow(entity_id=uri, entity_label=uri, entity_type="entity", dataset="dataset") for uri in entity_selection])

    print("Total DBProp mappings:", len(DBProp_DIRECT_MAPPINGS))
    print("Total DBOnto mappings:", len(DBOnto_DIRECT_MAPPINGS))

    print(entity_acq_dir / "reference")

    input_store = FileHashStore2(base_dir=entity_acq_dir / "reference", cache_enabled=True, cache_maxsize=100_000, cache_return_copy=False)

    # bundle reference
    if split.kg_reference is not None: 
        bundle_reference(split.kg_reference, entity_selection, input_store, idx)
    else:
        raise ValueError("kg_reference is None")

    # bundle reference
    if split.kg_seed is not None:
        bundle_seed(split.kg_seed, entity_selection, input_store)
    else:
        raise ValueError("kg_seed is None")

    # bundle sources
    source_types = [SourceType.rdf, SourceType.json, SourceType.text]
    split.set_sources(source_types)

    bundle_text_source(split.sources[SourceType.text], entity_selection, entity_acq_dir.as_posix())
    bundle_rdf_source(split.sources[SourceType.rdf], entity_selection, input_store, idx)
    bundle_json_source(split.sources[SourceType.json], entity_selection, entity_acq_dir.as_posix())


def generate_inc_movie_kgb():

    entity_list_path = ENTITY_LIST_PATH
    entity_acq_dir = ACQ_DIR

    subsets = generate_overlap_xyz(entity_list_path, NUM_SUBSETS, OVERLAP_RATIO, SUBSET_SIZE)

    ds = Dataset(root=BUNDLE_DIR)
    ds.set_entities_master(open(entity_list_path, "r").readlines())

    ds.set_splits(0, len(subsets))

    for idx, subset in enumerate(subsets):
        print(f"doing split {idx}")
        if idx >     4:
            continue
        generate_split_bundles(subset, idx, ds, entity_acq_dir)

def test_generate_inc_movie_kgb():
    generate_inc_movie_kgb()

def evaluate_inc_movie_kgb():
    pass

def test_convert_to_json():
    uri_list = read_uri_list_file(ENTITY_LIST_PATH)
    input_store = FileHashStore2(base_dir=(ACQ_DIR / "reference").as_posix(), cache_enabled=True, cache_maxsize=100_000, cache_return_copy=False)
    output_dir_tmp = ACQ_DIR / "json_tmp"
    output_dir = ACQ_DIR / "json"

    shutil.rmtree(output_dir_tmp, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)

    os.makedirs(output_dir_tmp, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    mappings = DBProp_DIRECT_MAPPINGS
    # mappings[f"{NAMESPACE_FOAF}name"] = DirectMappingType.LITERAL

    construct_graph_from_root_uris(
        uri_list,
        mappings, 
        input_store, 
        FileHashStore2(base_dir=output_dir_tmp.as_posix()), 
    )

    tmp_store = FileHashStore2(base_dir=output_dir_tmp.as_posix())
    for uri in tqdm(uri_list):
        graph = tmp_store.retrieve(uri)
        jsondata = build_recursive_json(uri, graph)
        with open(os.path.join(output_dir, hash_uri(uri)+".json"), "w") as f:
            json.dump(jsondata, f, indent=4)


from pyodibel.rdf_ops.extract import extract_subgraph_recursive


def get_all_entities_with_types(film_subset, seed_dir) -> dict[str, str]:
    entities_with_types = {}
    graph = Graph()
    for file in os.listdir(seed_dir):
        if file.endswith(".nt"):
            graph.parse(os.path.join(seed_dir, file), format="nt")

    for uri in film_subset:
        subgraph = extract_subgraph_recursive(uri,graph)
        s, _, t = list(subgraph.triples((None, RDF.type, None)))[0]
        entities_with_types[str(s)] = str(t)

    return entities_with_types

# def test_generate_split_matches():
#     # TODO get all entities from reference and calculate overlap with each split

#     ds = load_dataset(BUNDLE_DIR)

#     full_subsets_named: dict[str, dict[str, str]] = {}

#     film_subsets_named: dict[str, list[str]] = {}

#     for split in ds.splits.values():

#         print(split.index.entities_csv.as_posix())
#         film_subsets_named[split.root.name] = list(open(split.index.entities_csv.as_posix()).readlines())

#         reference = split.kg_reference
#         if reference is None:
#             raise ValueError("kg_reference is None")
        

#         pos_entities = reference.meta.entities
#         if pos_entities is None:
#             raise ValueError("pos_entities is None")
        
#         entities = [e.entity_id for e in pos_entities.read_csv()]
        
#         subset = get_all_entities_with_types(entities, reference.data.dir)
#         full_subsets_named[split.root.name] = subset

#     film_subsets = [film_subsets_named[f"split_{i}"] for i in range(len(film_subsets_named))]

#     print(validate_overlaps(film_subsets))

#     from itertools import combinations

#     overlap_ratios = {}

#     matches = []

#     keys = list(full_subsets_named.keys())

#     for i, j in combinations(range(len(keys)), 2):
#         overlap = set(full_subsets_named[keys[i]]) & set(full_subsets_named[keys[j]])
#         for uri in overlap:
#             matches.append(MatchesRow(left_dataset=keys[i], left_id=uri, right_dataset=keys[j], right_id=uri, match_type="exact"))

#         ratio = len(overlap) / len(full_subsets_named[keys[i]])
#         overlap_ratios[f"{keys[i]}-{keys[j]}"] = ratio
    
#     with open(BUNDLE_DIR / "split_match_entities.csv", "w") as f:
#         f.write("left_dataset\tleft_id\tright_dataset\tright_id\n")
#         for match in matches:
#             f.write(f"{match.left_dataset}\t{match.left_id}\t{match.right_dataset}\t{match.right_id}\n")

#     print(overlap_ratios)

def test_generate_meta_matches():

    ds = load_dataset(BUNDLE_DIR)

    splits = ds.splits.values()

    def get_split_idx(split):
        return int(split.root.name.split("_")[1])

    for left_split in splits:

        match_rows = []

        for right_split in splits:
            if left_split.root.name == right_split.root.name:
                continue

            if left_split.kg_seed is None or right_split.kg_seed is None:
                continue

            from_rdf_graph_path = left_split.kg_seed.root / "data.nt"
            to_rdf_graph_path = right_split.kg_seed.root / "data.nt"

            from_rdf_graph = Graph()
            from_rdf_graph.parse(from_rdf_graph_path.as_posix(), format="nt")
            to_rdf_graph = Graph()
            to_rdf_graph.parse(to_rdf_graph_path.as_posix(), format="nt")

            from_entities = [str(s) for s in from_rdf_graph.subjects(unique=True)]
            to_entities = [str(s) for s in to_rdf_graph.subjects(unique=True)]
        

            intersect = set(from_entities) & set(to_entities)

            intersect_types = {}

            for intersect_entity in intersect:
                et = from_rdf_graph.value(URIRef(intersect_entity), RDF.type)
                if et is None:
                    intersect_types[intersect_entity] = OWL.Thing
                else:
                    intersect_types[intersect_entity] = str(et)

            intersect_film_entites = [e for e in intersect_types.keys() if "http://kg.org/ontology/Film" == intersect_types[e]]
                
            print(len(from_entities), len(to_entities))
            all_ratio = len(intersect) / len(from_entities)
            film_ratio = len(intersect_film_entites) / len(from_entities)
            print(f"{left_split.root.name} -> {right_split.root.name}: {all_ratio} {film_ratio}")

            for uri in intersect_types:
                match_rows.append(MatchesRow(
                    left_dataset=left_split.root.name+"/kg/seed", 
                    left_id=uri, 
                    right_dataset=right_split.root.name+"/kg/seed", 
                    right_id=uri,
                    entity_type=intersect_types[uri]))
            

        # seed to seed
        left_split.kg_seed.meta.set_matches(match_rows)

        # workaround to set for rdf dataset to seed

        adapted_match_rows = []
        for mr in match_rows:
            idx = left_split.root.name.split("/")[0].split("_")[1]

            amr = MatchesRow(
                left_dataset=mr.left_dataset.replace("kg/seed", "kg/rdf"),
                left_id=mr.left_id.replace("kg.org/resource/", f"kg.org/rdf/{idx}/resource/"),
                right_dataset=mr.right_dataset,
                right_id=mr.right_id,
                entity_type=mr.entity_type)

            adapted_match_rows.append(amr)

        left_split.sources[SourceType.rdf].meta.set_matches(adapted_match_rows)
