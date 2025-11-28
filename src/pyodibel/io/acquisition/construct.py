import os
import json
import hashlib
from pathlib import Path
from typing import Dict, Set, Optional, List, Union
from urllib.parse import quote
from rdflib import Graph, URIRef, RDFS, RDF


def build_recursive_json(subject: str, graph: Graph, visited: Optional[Set[str]] = None, max_depth: int = 3, current_depth: int = 0) -> Optional[Dict[str, Union[str, List[str], Dict]]]:
    """
    Build recursive JSON representation of an RDF subject.
    
    Args:
        subject: Subject URI to build JSON for
        graph: RDF graph containing the data
        visited: Set of already visited URIs to prevent cycles
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth
    
    Returns:
        Dictionary representation of the subject, or None if cycle detected. Values are strings or lists of strings.
    """
    if visited is None:
        visited = set()
    
    if subject in visited or current_depth >= max_depth:
        return None  # Prevent infinite loops or excessive depth
    
    visited.add(subject)
    result = {}
    
    # Get all triples for this subject
    for _, p, o in graph.triples((URIRef(subject), None, None)):
        # Extract key from predicate URI
        predicate_uri = str(p)
        key = predicate_uri.split("#")[-1] if "#" in predicate_uri else predicate_uri.split("/")[-1]
        
        def get_value(obj) -> Union[str, Dict, None]:
            if isinstance(obj, URIRef):
                # Recursively build JSON for object URIs
                nested_json = build_recursive_json(str(obj), graph, visited, max_depth, current_depth + 1)
                if nested_json:
                    return nested_json
                else:
                    # If recursion failed, just return the URI as a string
                    return str(obj)
            else:
                # Return literal value as string
                return str(obj)
        
        value = get_value(o)
        
        # Handle multiple values for the same predicate
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            if value and value != "" and value != {}:
                result[key].append(value)
        else:
            if value and value != "" and value != {}:
                result[key] = value
    
    # Add provenance hash
    result["provenance_hash"] = hashlib.sha256(subject.encode("utf-8")).hexdigest()
    
    return result


def build_comprehensive_json(
    main_subject_uri: str,
    entity_dir: Path,
    allowed_namespaces: Set[str],
    max_depth: int,
    uri_labels: Dict[str, str]
) -> Optional[Dict]:
    """
    Build comprehensive JSON representation including nested entities.
    
    Args:
        main_subject_uri: URI of the main entity
        entity_dir: Directory containing the main entity and nested entities
        allowed_namespaces: Set of namespace URIs to filter by
        max_depth: Maximum recursion depth
        uri_labels: Dictionary mapping URIs to their labels
    
    Returns:
        Comprehensive JSON representation with nested entities
    """
    # Start with the main entity data
    main_data_file = entity_dir / "data.nt"
    
    if not main_data_file.exists():
        return None
    
    # Read and parse main entity data
    with open(main_data_file, 'r', encoding='utf-8') as f:
        main_data = f.read()
    
    if not main_data.strip():
        return None
    
    # Parse main entity into graph
    main_graph = parse_ntriples_to_graph(main_data)
    filtered_main_graph = filter_graph_by_namespaces(main_graph, allowed_namespaces)
    
    if len(filtered_main_graph) == 0:
        return None
    
    # Build comprehensive graph with all nested entities
    comprehensive_graph = Graph()
    comprehensive_graph += filtered_main_graph
    
    # Load all nested entity data
    for nested_dir in entity_dir.iterdir():
        if not nested_dir.is_dir() or nested_dir.name == "data.nt":
            continue
        
        nested_data_file = nested_dir / "data.nt"
        if nested_data_file.exists():
            try:
                with open(nested_data_file, 'r', encoding='utf-8') as f:
                    nested_data = f.read()
                
                if nested_data.strip():
                    nested_graph = parse_ntriples_to_graph(nested_data)
                    filtered_nested_graph = filter_graph_by_namespaces(nested_graph, allowed_namespaces)
                    comprehensive_graph += filtered_nested_graph
                    
            except Exception as e:
                print(f"Warning: Error loading nested entity {nested_dir.name}: {e}")
    
    # Build JSON representation with comprehensive graph
    json_data = build_recursive_json(main_subject_uri, comprehensive_graph, max_depth=max_depth)
    
    if json_data:
        # Add metadata about the comprehensive structure
        json_data["_metadata"] = {
            "main_entity": main_subject_uri,
            "nested_entities_count": len([d for d in entity_dir.iterdir() if d.is_dir() and d.name != "data.nt"]),
            "total_triples": len(comprehensive_graph),
            "filtered_namespaces": list(allowed_namespaces)
        }
    
    return json_data


def filter_graph_by_namespaces(graph: Graph, allowed_namespaces: Set[str]) -> Graph:
    """
    Filter a graph to only keep triples with predicates from allowed namespaces.
    
    Args:
        graph: rdflib Graph to filter
        allowed_namespaces: Set of namespace URIs to keep
    
    Returns:
        Filtered rdflib Graph
    """
    filtered_graph = Graph()
    
    for s, p, o in graph:
        predicate_uri = str(p)
        
        # Check if predicate starts with any allowed namespace
        if any(predicate_uri.startswith(namespace) for namespace in allowed_namespaces):
            filtered_graph.add((s, p, o))
    
    return filtered_graph


def parse_ntriples_to_graph(ntriples_data: str) -> Graph:
    """Parse N-Triples data into an rdflib Graph."""
    graph = Graph()
    try:
        graph.parse(data=ntriples_data, format="ntriples")
    except Exception as e:
        print(f"Warning: Error parsing N-Triples data: {e}")
    return graph


def load_uri_labels_from_fetched_data(base_dir: str) -> Dict[str, str]:
    """
    Load URI to label mappings from fetched data directory structure.
    
    Args:
        base_dir: Base directory containing fetched data with nested structure
    
    Returns:
        Dictionary mapping URIs to their labels
    """
    uri_labels = {}
    base_path = Path(base_dir)
    
    # Walk through all data.nt files
    for data_file in base_path.rglob("data.nt"):
        try:
            # Parse the data file
            graph = Graph()
            graph.parse(str(data_file), format="ntriples")
            
            # Get the subject URI from the first triple (assuming all triples have same subject)
            if len(graph) > 0:
                subject = list(graph.subjects())[0]
                subject_uri = str(subject)
                
                # Look for rdfs:label triples
                for s, p, o in graph.triples((subject, RDFS.label, None)):
                    # Check if it's an English label
                    if hasattr(o, 'language') and getattr(o, 'language', None) == 'en':
                        uri_labels[subject_uri] = str(o)
                        break
                else:
                    # If no English label, take the first label
                    for s, p, o in graph.triples((subject, RDFS.label, None)):
                        uri_labels[subject_uri] = str(o)
                        break
                        
        except Exception as e:
            print(f"Warning: Error loading labels from {data_file}: {e}")
    
    print(f"Loaded {len(uri_labels)} URI to label mappings")
    return uri_labels


def get_label_or_name(uri, graph, uri_labels):
    # Try label first, then name, else fallback to last part of URI
    if uri in uri_labels:
        return uri_labels[uri]
    for _, _, o in graph.triples((URIRef(uri), RDFS.label, None)):
        return str(o)
    for _, _, o in graph.triples((URIRef(uri), URIRef("http://dbpedia.org/property/name"), None)):
        return str(o)
    return uri.split("/")[-1]

def build_flat_json(subject, graph, uri_labels):
    """
    Build a flat JSON dict for the given subject in the graph.
    All values are strings or lists of strings. Object URIs are replaced by their label/name if available.
    """
    result = {}
    for _, p, o in graph.triples((URIRef(subject), None, None)):
        pred_uri = str(p)
        key = pred_uri.split("#")[-1] if "#" in pred_uri else pred_uri.split("/")[-1]
        if isinstance(o, URIRef):
            value = get_label_or_name(str(o), graph, uri_labels)
        else:
            value = str(o)
        # Collect all values for each key
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            result[key].append(value)
        else:
            result[key] = value
    # Convert single-item lists to string
    for k, v in list(result.items()):
        if isinstance(v, list) and len(v) == 1:
            result[k] = v[0]
    return result

def construct_flat_json_source(
    input_dir: str,
    output_dir: str,
    allowed_namespaces: Optional[Set[str]] = None,
    include_labels: bool = True,
    dry_run: bool = False
):
    """
    Build a flat JSON for the main entity in input_dir and write to output_dir.
    """
    if allowed_namespaces is None:
        allowed_namespaces = {
            "http://dbpedia.org/property/",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#"
        }
    if include_labels:
        allowed_namespaces.add("http://www.w3.org/2000/01/rdf-schema#")
    uri_labels = load_uri_labels_from_fetched_data(input_dir)
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    main_data_file = input_path / "data.nt"
    if not main_data_file.exists():
        print(f"No data.nt in {input_dir}")
        return
    with open(main_data_file, 'r', encoding='utf-8') as f:
        main_data = f.read()
    main_graph = parse_ntriples_to_graph(main_data)
    filtered_graph = filter_graph_by_namespaces(main_graph, allowed_namespaces)
    if len(filtered_graph) == 0:
        print(f"No triples after filtering in {input_dir}")
        return
    main_subject = list(filtered_graph.subjects())[0]
    flat_json = build_flat_json(str(main_subject), filtered_graph, uri_labels)
    # Output file name is the directory name
    entity_hash = input_path.name
    output_file = output_path / f"{entity_hash}.json"
    if not dry_run:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(flat_json, f, indent=2, ensure_ascii=False)
        print(f"Wrote flat JSON to {output_file}")
    return flat_json


def construct_json_source(
    input_dir: str,
    output_dir: str,
    allowed_namespaces: Optional[Set[str]] = None,
    max_depth: int = 3,
    include_labels: bool = True,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Construct JSON representations from fetched RDF data.
    
    Args:
        input_dir: Directory containing fetched data with hash directory structure
        output_dir: Directory to store JSON files
        allowed_namespaces: Set of namespace URIs to filter by (default: dbpedia.org/property and rdfs/rdf)
        max_depth: Maximum recursion depth for JSON construction
        include_labels: Whether to include rdfs:label triples
        dry_run: If True, only count entities without writing files
    
    Returns:
        Dictionary with processing statistics
    """
    # Default namespaces if none provided
    if allowed_namespaces is None:
        allowed_namespaces = {
            "http://dbpedia.org/property/",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#"
        }
    
    # Add rdfs:label namespace if labels should be included
    if include_labels:
        allowed_namespaces.add("http://www.w3.org/2000/01/rdf-schema#")
    
    print(f"Filtering by namespaces: {allowed_namespaces}")
    print(f"Max recursion depth: {max_depth}")
    
    # Load URI labels for better JSON representation
    uri_labels = load_uri_labels_from_fetched_data(input_dir)
    
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not dry_run:
        output_path.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "files_processed": 0,
        "json_files_created": 0,
        "entities_with_data": 0,
        "entities_empty": 0,
        "total_triples_before": 0,
        "total_triples_after": 0
    }
    
    print(f"Processing fetched data from: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # Process the input directory as a single entity directory
    if not input_path.is_dir():
        print(f"Error: {input_dir} is not a directory")
        return stats
    
    # Check if this is an entity directory (contains data.nt)
    main_data_file = input_path / "data.nt"
    
    if not main_data_file.exists():
        print(f"⚠️  No data.nt file found in {input_path}")
        return stats
    
    try:
        # Read the main entity data
        with open(main_data_file, 'r', encoding='utf-8') as f:
            main_data = f.read()
        
        if not main_data.strip():
            stats["entities_empty"] += 1
            return stats
        
        # Parse main entity into graph
        main_graph = parse_ntriples_to_graph(main_data)
        original_triple_count = len(main_graph)
        stats["total_triples_before"] += original_triple_count
        
        # Filter the main graph by namespaces
        filtered_main_graph = filter_graph_by_namespaces(main_graph, allowed_namespaces)
        filtered_triple_count = len(filtered_main_graph)
        stats["total_triples_after"] += filtered_triple_count
        
        if filtered_triple_count == 0:
            stats["entities_empty"] += 1
            print(f"⏭️  {input_path.name}: No triples after filtering")
            return stats
        
        # Get the main subject URI
        main_subject = list(filtered_main_graph.subjects())[0]
        main_subject_uri = str(main_subject)
        
        # Build comprehensive JSON with nested entities
        json_data = build_comprehensive_json(
            main_subject_uri, 
            input_path, 
            allowed_namespaces, 
            max_depth, 
            uri_labels
        )
        
        if json_data:
            stats["entities_with_data"] += 1
            
            # Use the directory name as the filename
            entity_hash = input_path.name
            output_filename = f"{entity_hash}.json"
            output_file = output_path / output_filename
            
            if not dry_run:
                # Write JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                
                stats["json_files_created"] += 1
                print(f"✅ {output_filename}: {original_triple_count} -> {filtered_triple_count} triples")
            else:
                print(f"📝 {output_filename}: {original_triple_count} -> {filtered_triple_count} triples (dry run)")
        else:
            stats["entities_empty"] += 1
            print(f"❌ {input_path.name}: Failed to build JSON")
        
        stats["files_processed"] += 1
        
    except Exception as e:
        print(f"❌ Error processing {input_path}: {e}")
    
    # Print summary
    print(f"\n=== JSON Construction Summary ===")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Entities with data: {stats['entities_with_data']}")
    print(f"Empty entities: {stats['entities_empty']}")
    print(f"JSON files created: {stats['json_files_created']}")
    print(f"Total triples before filtering: {stats['total_triples_before']}")
    print(f"Total triples after filtering: {stats['total_triples_after']}")
    
    if stats['total_triples_before'] > 0:
        reduction_percent = ((stats['total_triples_before'] - stats['total_triples_after']) / 
                           stats['total_triples_before']) * 100
        print(f"Triple reduction: {reduction_percent:.1f}%")
    
    return stats


def construct_json_from_predicates_file(
    input_dir: str,
    output_dir: str,
    predicates_file: str,
    max_depth: int = 3,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Construct JSON representations using predicates from a file.
    
    Args:
        input_dir: Directory containing fetched data
        output_dir: Directory to store JSON files
        predicates_file: File containing predicate URIs (one per line)
        max_depth: Maximum recursion depth for JSON construction
        dry_run: If True, only count entities without writing files
    
    Returns:
        Dictionary with processing statistics
    """
    # Read predicates from file
    allowed_predicates = set()
    try:
        with open(predicates_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    allowed_predicates.add(line)
        print(f"Loaded {len(allowed_predicates)} predicates from {predicates_file}")
    except FileNotFoundError:
        print(f"Error: Predicates file '{predicates_file}' not found.")
        return {}
    except Exception as e:
        print(f"Error reading predicates file: {e}")
        return {}
    
    # Convert predicates to namespaces for filtering
    allowed_namespaces = set()
    for predicate in allowed_predicates:
        # Extract namespace from predicate URI
        if '#' in predicate:
            namespace = predicate.split('#')[0] + '#'
        else:
            namespace = '/'.join(predicate.split('/')[:-1]) + '/'
        allowed_namespaces.add(namespace)
    
    return construct_json_source(
        input_dir=input_dir,
        output_dir=output_dir,
        allowed_namespaces=allowed_namespaces,
        max_depth=max_depth,
        dry_run=dry_run
    )


def construct_flat_json_from_filtered_data(
    input_dir: str,
    output_dir: str,
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Construct flat JSON from pre-filtered RDF data where URIs have been replaced with labels.
    
    Args:
        input_dir: Directory containing filtered data.nt files
        output_dir: Directory to store JSON files
        dry_run: If True, only return the JSON without writing files
    
    Returns:
        Flat JSON dictionary or None if no data
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not dry_run:
        output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all data.nt files in the filtered directory
    data_files = list(input_path.rglob("data.nt"))
    
    if not data_files:
        print(f"No data.nt files found in {input_dir}")
        return None
    
    # Process the main entity file first
    main_data_file = input_path / "data.nt"
    if not main_data_file.exists():
        print(f"No main data.nt file found in {input_dir}")
        return None
    
    # Read and parse the filtered main entity data
    with open(main_data_file, 'r', encoding='utf-8') as f:
        main_data = f.read()
    
    if not main_data.strip():
        print(f"Empty main data.nt file in {input_dir}")
        return None
    
    # Parse the filtered data
    main_graph = parse_ntriples_to_graph(main_data)
    
    if len(main_graph) == 0:
        print(f"No triples in filtered data from {input_dir}")
        return None
    
    # Get the main subject
    main_subject = list(main_graph.subjects())[0]
    main_subject_uri = str(main_subject)
    
    # Build flat JSON from the filtered data
    # Since the data is already filtered, URIs should be replaced with labels
    flat_json = {}
    
    for s, p, o in main_graph:
        if s == main_subject:  # Only process triples about the main entity
            pred_uri = str(p)
            key = pred_uri.split("#")[-1] if "#" in pred_uri else pred_uri.split("/")[-1]
            
            # Handle the object value
            if isinstance(o, URIRef):
                # This should be a label since the data is filtered
                value = str(o)
            else:
                # This is a literal
                value = str(o)
            
            # Collect all values for each key
            if key in flat_json:
                if not isinstance(flat_json[key], list):
                    flat_json[key] = [flat_json[key]]
                flat_json[key].append(value)
            else:
                flat_json[key] = value
    
    # Convert single-item lists to string
    for k, v in list(flat_json.items()):
        if isinstance(v, list) and len(v) == 1:
            flat_json[k] = v[0]
    
    # Add metadata
    flat_json["_metadata"] = {
        "main_entity": main_subject_uri,
        "total_triples": len(main_graph),
        "source": "filtered_rdf_data"
    }
    
    # Output file name is the directory name
    entity_hash = input_path.name
    output_file = output_path / f"{entity_hash}.json"
    
    if not dry_run:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(flat_json, f, indent=2, ensure_ascii=False)
        print(f"Wrote flat JSON to {output_file}")
    
    return flat_json


if __name__ == "__main__":
    # Example usage
    INPUT_DIR = "src/pyodibel/tests_data/filter/58b4456d6ff046f1482938342bb28391"
    OUTPUT_DIR = "./json_representations"
    
    # Example 1: Construct JSON with default namespaces
    print("=== Constructing JSON with Default Namespaces ===")
    stats = construct_json_source(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        max_depth=2
    )
    
    # Example 2: Construct JSON with custom namespaces
    print("\n=== Constructing JSON with Custom Namespaces ===")
    custom_namespaces = {
        "http://dbpedia.org/property/",
        "http://dbpedia.org/ontology/",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    }
    
    stats2 = construct_json_source(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_custom",
        allowed_namespaces=custom_namespaces,
        max_depth=3
    )
    
    # Example 3: Construct JSON from predicates file
    print("\n=== Constructing JSON from Predicates File ===")
    stats3 = construct_json_from_predicates_file(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_file",
        predicates_file="movie_kg_predicates.txt",
        max_depth=2
    )