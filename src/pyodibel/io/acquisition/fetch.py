import hashlib
import os
import re
from typing import List, Set, Dict, Optional
from rdflib import Graph, URIRef, Literal
from pyodibel.dbaccess import get_data_of_uris, decompress_gzip


def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()


def parse_ntriples_to_graph(ntriples_data: str) -> Graph:
    """Parse N-Triples data into an rdflib Graph."""
    graph = Graph()
    try:
        graph.parse(data=ntriples_data, format="ntriples")
    except Exception as e:
        print(f"Warning: Error parsing N-Triples data: {e}")
    return graph


def extract_object_uris_from_graph(graph: Graph, target_predicates: Set[str]) -> Set[str]:
    """
    Extract object URIs from graph where predicates match target_predicates.
    
    Args:
        graph: rdflib Graph containing RDF data
        target_predicates: Set of predicate URIs to match
    
    Returns:
        Set of object URIs that are also URIs (not literals)
    """
    object_uris = set()
    
    for s, p, o in graph:
        predicate_uri = str(p)
        if predicate_uri in target_predicates and isinstance(o, URIRef):
            object_uris.add(str(o))
    
    return object_uris


def recursive_fetch_entities(
    uris: Set[str], 
    db_url: str, 
    target_predicates: Set[str],
    output_dir: str = "./data",
    max_depth: int = 3,
    visited_uris: Optional[Set[str]] = None,
    current_depth: int = 0,
    parent_dir: Optional[str] = None
) -> Dict[str, List[str]]:
    """
    Recursively fetch entity data and store in hierarchical structure.
    
    Args:
        uris: Set of URIs to fetch
        db_url: Database connection URL
        target_predicates: Set of predicate URIs that trigger recursive fetching
        output_dir: Base directory to store the files
        max_depth: Maximum recursion depth
        visited_uris: Set of already visited URIs (for cycle detection)
        current_depth: Current recursion depth
        parent_dir: Directory of the parent entity (for nested storage)
    
    Returns:
        Dictionary mapping entity URIs to lists of stored file paths
    """
    if visited_uris is None:
        visited_uris = set()
    
    if current_depth >= max_depth:
        print(f"⚠️  Max depth {max_depth} reached, stopping recursion")
        return {}
    
    # Determine the current working directory
    current_dir = parent_dir if parent_dir else output_dir
    os.makedirs(current_dir, exist_ok=True)
    
    # Fetch data from database
    data = get_data_of_uris(uris, db_url)
    
    successful_files = {}
    
    for uri, rdf_data_compressed in data:
        if uri in visited_uris:
            print(f"⏭️  Skipping already visited URI: {uri}")
            continue
            
        try:
            # Decompress the RDF data
            rdf_data = decompress_gzip(rdf_data_compressed)
            
            # Parse into rdflib Graph
            graph = parse_ntriples_to_graph(rdf_data)
            
            # Hash the URI for directory name
            entity_hash = hash_uri(uri)
            entity_dir = os.path.join(current_dir, entity_hash)
            os.makedirs(entity_dir, exist_ok=True)
            
            # Store the main entity data
            data_file_path = os.path.join(entity_dir, "data.nt")
            with open(data_file_path, 'w', encoding='utf-8') as f:
                f.write(rdf_data)
            
            print(f"✅ Stored: {uri} -> {data_file_path}")
            
            # Track successful files
            if uri not in successful_files:
                successful_files[uri] = []
            successful_files[uri].append(data_file_path)
            
            # Mark as visited
            visited_uris.add(uri)
            
            # Extract object URIs for recursive fetching
            object_uris = extract_object_uris_from_graph(graph, target_predicates)
            
            if object_uris and current_depth < max_depth - 1:
                print(f"🔍 Found {len(object_uris)} object URIs for recursive fetching")
                
                # Recursively fetch object URIs within this entity's directory
                object_uris_to_fetch = set()
                for obj_uri in object_uris:
                    if obj_uri not in visited_uris:
                        obj_hash = hash_uri(obj_uri)
                        obj_dir = os.path.join(entity_dir, obj_hash)
                        
                        # Check if we already have this object's data
                        if not os.path.exists(os.path.join(obj_dir, "data.nt")):
                            object_uris_to_fetch.add(obj_uri)
                        else:
                            print(f"📁 Object {obj_uri} already exists in {obj_dir}")
                
                # Recursively fetch object URIs within this entity's directory
                if object_uris_to_fetch:
                    print(f"🔄 Recursively fetching {len(object_uris_to_fetch)} URIs at depth {current_depth + 1}")
                    recursive_results = recursive_fetch_entities(
                        object_uris_to_fetch,
                        db_url,
                        target_predicates,
                        output_dir,
                        max_depth,
                        visited_uris,
                        current_depth + 1,
                        entity_dir  # Pass the current entity's directory as parent for nested storage
                    )
                    
                    # Merge results
                    for obj_uri, files in recursive_results.items():
                        if obj_uri not in successful_files:
                            successful_files[obj_uri] = []
                        successful_files[obj_uri].extend(files)
            
        except Exception as e:
            print(f"❌ Failed to process {uri}: {e}")
    
    # Write URI->hash mapping to file (only at root level)
    if current_depth == 0:
        mapping_file = os.path.join(output_dir, "uri_hash_mapping.txt")
        with open(mapping_file, 'w', encoding='utf-8') as f:
            for uri in visited_uris:
                hash_value = hash_uri(uri)
                f.write(f"{uri}\t{hash_value}\n")
        
        print(f"📝 URI->hash mapping saved to: {mapping_file}")
    
    print(f"\n📊 Summary: {len(successful_files)} entities processed at depth {current_depth}")
    
    return successful_files


def fetch_and_store_entities(uris: Set[str], db_url: str, output_dir: str = "./data") -> List[str]:
    """
    Fetch entity data from database and store each entity in a file.
    
    Args:
        uris: Set of URIs to fetch
        db_url: Database connection URL
        output_dir: Directory to store the files
    
    Returns:
        List of successfully created file paths
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Fetch data from database
    data = get_data_of_uris(uris, db_url)
    
    successful_files = []
    uri_hash_mapping = {}
    
    for uri, rdf_data_compressed in data:
        try:
            # Decompress the RDF data
            rdf_data = decompress_gzip(rdf_data_compressed)
            
            # Hash the URI for filename
            filename = hash_uri(uri)
            file_path = os.path.join(output_dir, f"{filename}.nt")
            
            # Store the URI->hash mapping
            uri_hash_mapping[uri] = filename
            
            # Write the RDF data to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(rdf_data)
            
            print(f"✅ Stored: {uri} -> {file_path}")
            successful_files.append(file_path)
            
        except Exception as e:
            print(f"❌ Failed to process {uri}: {e}")
    
    # Write URI->hash mapping to file
    mapping_file = os.path.join(output_dir, "uri_hash_mapping.txt")
    with open(mapping_file, 'w', encoding='utf-8') as f:
        for uri, hash_value in uri_hash_mapping.items():
            f.write(f"{uri}\t{hash_value}\n")
    
    print(f"📝 URI->hash mapping saved to: {mapping_file}")
    print(f"\n📊 Summary: {len(successful_files)}/{len(data)} entities successfully stored")
    return successful_files


if __name__ == "__main__":
    # Example usage
    DATABASE_URL = "postgresql+psycopg2://dbpedia:YohBeingoxe7@localhost/entityindexdb"
    
    # URIs to fetch
    uris_to_fetch = {
        # "https://wikidata.dbpedia.org/resource/Q82155",
        "http://dbpedia.org/resource/Angela_Merkel"
        # "http://www.wikidata.org/entity/Q64"
    }
    
    # Example 1: Simple fetch and store
    print("=== Simple Fetch and Store ===")
    stored_files = fetch_and_store_entities(uris_to_fetch, DATABASE_URL, "./entity_data")
    print(f"Stored files: {stored_files}")
    
    # Example 2: Recursive fetch with target predicates
    print("\n=== Recursive Fetch ===")
    target_predicates = {
        "http://dbpedia.org/ontology/birthPlace",
        "http://dbpedia.org/ontology/deathPlace", 
        "http://dbpedia.org/ontology/occupation",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    }
    
    recursive_results = recursive_fetch_entities(
        uris_to_fetch, 
        DATABASE_URL, 
        target_predicates, 
        "./recursive_entity_data",
        max_depth=2
    )
    
    print(f"Recursive fetch results: {recursive_results}")
