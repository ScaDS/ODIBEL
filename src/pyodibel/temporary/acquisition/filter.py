import os
import re
from pathlib import Path
from typing import Set, List, Dict, Optional
from rdflib import Graph, URIRef, Literal


def parse_ntriples_to_graph(ntriples_data: str) -> Graph:
    """Parse N-Triples data into an rdflib Graph."""
    graph = Graph()
    try:
        graph.parse(data=ntriples_data, format="ntriples")
    except Exception as e:
        print(f"Warning: Error parsing N-Triples data: {e}")
    return graph


def filter_graph_by_predicates(graph: Graph, allowed_predicates: Set[str]) -> Graph:
    """
    Filter a graph to only keep triples with predicates that match the allowed set.
    
    Args:
        graph: rdflib Graph to filter
        allowed_predicates: Set of predicate URIs to keep
    
    Returns:
        Filtered rdflib Graph
    """
    filtered_graph = Graph()
    
    for s, p, o in graph:
        predicate_uri = str(p)
        
        # Check if predicate matches any allowed predicate
        if any(allowed_predicate in predicate_uri for allowed_predicate in allowed_predicates):
            filtered_graph.add((s, p, o))
    
    return filtered_graph


def filter_graph_by_namespaces(graph: Graph, allowed_namespaces: Set[str]) -> Graph:
    """
    Filter a graph to only keep triples with predicates from allowed namespaces.
    
    Args:
        graph: rdflib Graph to filter
        allowed_namespaces: Set of namespace URIs to keep (e.g., {"http://dbpedia.org/ontology/"})
    
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


def filter_graph_by_predicates_and_namespaces(graph: Graph, allowed_predicates: Set[str], allowed_namespaces: Set[str]) -> Graph:
    """
    Filter a graph to only keep triples with predicates that match either allowed predicates OR allowed namespaces.
    
    Args:
        graph: rdflib Graph to filter
        allowed_predicates: Set of predicate URIs to keep
        allowed_namespaces: Set of namespace URIs to keep
    
    Returns:
        Filtered rdflib Graph
    """
    filtered_graph = Graph()
    
    for s, p, o in graph:
        predicate_uri = str(p)
        
        # Check if predicate matches any allowed predicate OR starts with any allowed namespace
        predicate_match = any(allowed_predicate in predicate_uri for allowed_predicate in allowed_predicates)
        namespace_match = any(predicate_uri.startswith(namespace) for namespace in allowed_namespaces)
        
        if predicate_match or namespace_match:
            filtered_graph.add((s, p, o))
    
    return filtered_graph


def filter_graph_by_types(graph: Graph, allowed_types: Set[str]) -> Graph:
    """
    Filter a graph to only keep type triples (rdf:type) for specific entity types.
    
    Args:
        graph: rdflib Graph to filter
        allowed_types: Set of type URIs to keep (e.g., {"http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Film"})
    
    Returns:
        Filtered rdflib Graph
    """
    from rdflib import RDF
    
    filtered_graph = Graph()
    
    for s, p, o in graph:
        # Check if this is a type triple (rdf:type)
        if p == RDF.type:
            object_uri = str(o)
            # Check if the object (type) matches any allowed type
            if object_uri in allowed_types:
                filtered_graph.add((s, p, o))
        else:
            # Keep non-type triples as well
            filtered_graph.add((s, p, o))
    
    return filtered_graph


def filter_graph_by_types_only(graph: Graph, allowed_types: Set[str]) -> Graph:
    """
    Filter a graph to only keep type triples (rdf:type) for specific entity types, excluding all other triples.
    
    Args:
        graph: rdflib Graph to filter
        allowed_types: Set of type URIs to keep (e.g., {"http://dbpedia.org/ontology/Person", "http://dbpedia.org/ontology/Film"})
    
    Returns:
        Filtered rdflib Graph containing only type triples
    """
    from rdflib import RDF
    
    filtered_graph = Graph()
    
    for s, p, o in graph:
        # Only keep type triples (rdf:type) for allowed types
        if p == RDF.type:
            object_uri = str(o)
            # Check if the object (type) matches any allowed type
            if any(allowed_type in object_uri for allowed_type in allowed_types):
                filtered_graph.add((s, p, o))
    
    return filtered_graph


def load_uri_labels_from_fetched_data(base_dir: str) -> Dict[str, str]:
    """
    Load URI to label mappings from fetched data directory structure.
    
    Args:
        base_dir: Base directory containing fetched data with nested structure
    
    Returns:
        Dictionary mapping URIs to their labels
    """
    from rdflib import RDFS
    import hashlib
    
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
                    if isinstance(o, Literal) and hasattr(o, 'language') and o.language == 'en':
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


def replace_objects_with_labels(graph: Graph, uri_labels: Dict[str, str], target_predicates: Set[str]) -> Graph:
    """
    Replace object URIs with their labels for specific predicates.
    
    Args:
        graph: rdflib Graph to process
        uri_labels: Dictionary mapping URIs to their labels
        target_predicates: Set of predicate URIs to replace objects for
    
    Returns:
        Modified rdflib Graph with object URIs replaced by labels
    """
    from rdflib import Literal
    
    modified_graph = Graph()
    
    for s, p, o in graph:
        predicate_uri = str(p)
        
        # Check if this predicate should have objects replaced
        if any(target_predicate in predicate_uri for target_predicate in target_predicates):
            # Check if object is a URI and we have a label for it
            if isinstance(o, URIRef):
                object_uri = str(o)
                if object_uri in uri_labels:
                    # Replace URI with label literal
                    label = uri_labels[object_uri]
                    modified_graph.add((s, p, Literal(label, lang="en")))
                else:
                    # Keep original if no label found
                    modified_graph.add((s, p, o))
            else:
                # Keep non-URI objects as they are
                modified_graph.add((s, p, o))
        else:
            # Keep triples with non-target predicates as they are
            modified_graph.add((s, p, o))
    
    return modified_graph


def process_fetched_data(
    input_dir: str,
    output_dir: str,
    allowed_predicates: Optional[Set[str]] = None,
    allowed_namespaces: Optional[Set[str]] = None,
    allowed_types: Optional[Set[str]] = None,
    types_only: bool = False,
    replace_objects: Optional[Set[str]] = None,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Process fetched data and filter RDF triples based on predicates, namespaces, and/or types.
    
    Args:
        input_dir: Directory containing fetched data with data.nt files
        output_dir: Directory to store filtered data
        allowed_predicates: Set of predicate URIs to keep
        allowed_namespaces: Set of namespace URIs to keep
        allowed_types: Set of type URIs to keep (e.g., {"http://dbpedia.org/ontology/Person"})
        types_only: If True, only keep type triples for allowed types (excludes other triples)
        replace_objects: Set of predicate URIs whose object URIs should be replaced with labels
        dry_run: If True, only count triples without writing files
    
    Returns:
        Dictionary with processing statistics
    """
    if allowed_predicates is None and allowed_namespaces is None and allowed_types is None:
        raise ValueError("At least one of allowed_predicates, allowed_namespaces, or allowed_types must be provided")
    
    # Initialize empty sets if None
    if allowed_predicates is None:
        allowed_predicates = set()
    if allowed_namespaces is None:
        allowed_namespaces = set()
    if allowed_types is None:
        allowed_types = set()
    
    # Determine filtering mode
    use_predicates = len(allowed_predicates) > 0
    use_namespaces = len(allowed_namespaces) > 0
    use_types = len(allowed_types) > 0
    use_object_replacement = replace_objects is not None and len(replace_objects) > 0
    
    if use_types and types_only:
        print(f"Filtering by {len(allowed_types)} specific types (types only)")
    elif use_predicates and use_namespaces and use_types:
        print(f"Filtering by {len(allowed_predicates)} predicates AND {len(allowed_namespaces)} namespaces AND {len(allowed_types)} types")
    elif use_predicates and use_namespaces:
        print(f"Filtering by {len(allowed_predicates)} specific predicates AND {len(allowed_namespaces)} namespaces")
    elif use_predicates and use_types:
        print(f"Filtering by {len(allowed_predicates)} specific predicates AND {len(allowed_types)} types")
    elif use_namespaces and use_types:
        print(f"Filtering by {len(allowed_namespaces)} namespaces AND {len(allowed_types)} types")
    elif use_predicates:
        print(f"Filtering by {len(allowed_predicates)} specific predicates")
    elif use_namespaces:
        print(f"Filtering by {len(allowed_namespaces)} namespaces")
    elif use_types:
        print(f"Filtering by {len(allowed_types)} specific types")
    else:
        # Default to namespace filtering if none is explicitly provided
        allowed_namespaces = {
            "http://dbpedia.org/ontology/",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#"
        }
        use_namespaces = True
        print(f"Using default namespace filtering with {len(allowed_namespaces)} namespaces")
    
    # Load URI labels if object replacement is requested
    uri_labels = {}
    if use_object_replacement and replace_objects is not None:
        print(f"Loading URI labels for object replacement on {len(replace_objects)} predicates")
        uri_labels = load_uri_labels_from_fetched_data(input_dir)
    
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not dry_run:
        output_path.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "files_processed": 0,
        "total_triples_before": 0,
        "total_triples_after": 0,
        "files_with_data": 0,
        "files_empty": 0
    }
    
    print(f"Processing fetched data from: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # Walk through all data.nt files
    for data_file in input_path.rglob("data.nt"):
        try:
            # Read the original data
            with open(data_file, 'r', encoding='utf-8') as f:
                original_data = f.read()
            
            if not original_data.strip():
                stats["files_empty"] += 1
                continue
            
            # Parse into graph
            graph = parse_ntriples_to_graph(original_data)
            original_triple_count = len(graph)
            stats["total_triples_before"] += original_triple_count
            
            # Filter the graph based on mode
            if use_types and types_only:
                # Only keep type triples for allowed types
                filtered_graph = filter_graph_by_types_only(graph, allowed_types)
            elif use_predicates and use_namespaces and use_types:
                # Combine all three filtering approaches
                temp_graph = filter_graph_by_predicates_and_namespaces(graph, allowed_predicates, allowed_namespaces)
                filtered_graph = filter_graph_by_types(temp_graph, allowed_types)
            elif use_predicates and use_namespaces:
                # Combine predicate and namespace filtering
                filtered_graph = filter_graph_by_predicates_and_namespaces(graph, allowed_predicates, allowed_namespaces)
            elif use_predicates and use_types:
                # Combine predicate and type filtering
                temp_graph = filter_graph_by_predicates(graph, allowed_predicates)
                filtered_graph = filter_graph_by_types(temp_graph, allowed_types)
            elif use_namespaces and use_types:
                # Combine namespace and type filtering
                temp_graph = filter_graph_by_namespaces(graph, allowed_namespaces)
                filtered_graph = filter_graph_by_types(temp_graph, allowed_types)
            elif use_predicates:
                filtered_graph = filter_graph_by_predicates(graph, allowed_predicates)
            elif use_namespaces:
                filtered_graph = filter_graph_by_namespaces(graph, allowed_namespaces)
            elif use_types:
                filtered_graph = filter_graph_by_types(graph, allowed_types)
            else:
                # This should not happen due to the validation above
                raise ValueError("No filtering mode determined")
            
            # Apply object replacement if requested
            if use_object_replacement and replace_objects is not None:
                filtered_graph = replace_objects_with_labels(filtered_graph, uri_labels, replace_objects)
            
            filtered_triple_count = len(filtered_graph)
            stats["total_triples_after"] += filtered_triple_count
            
            # Calculate relative path for output
            rel_path = data_file.relative_to(input_path)
            output_file = output_path / rel_path
            
            if not dry_run:
                # Ensure output directory exists
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Write filtered data using rdflib serialization
                filtered_graph.serialize(destination=str(output_file), format="ntriples")
            
            stats["files_processed"] += 1
            if filtered_triple_count > 0:
                stats["files_with_data"] += 1
            
            print(f"✅ {rel_path}: {original_triple_count} -> {filtered_triple_count} triples")
            
        except Exception as e:
            print(f"❌ Error processing {data_file}: {e}")
    
    # Print summary
    print(f"\n=== Processing Summary ===")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Files with data: {stats['files_with_data']}")
    print(f"Empty files: {stats['files_empty']}")
    print(f"Total triples before: {stats['total_triples_before']}")
    print(f"Total triples after: {stats['total_triples_after']}")
    
    if stats['total_triples_before'] > 0:
        reduction_percent = ((stats['total_triples_before'] - stats['total_triples_after']) / 
                           stats['total_triples_before']) * 100
        print(f"Reduction: {reduction_percent:.1f}%")
    
    return stats


def construct_from_predicates_file(
    input_dir: str,
    output_dir: str,
    predicates_file: str,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Process fetched data using predicates from a file.
    
    Args:
        input_dir: Directory containing fetched data
        output_dir: Directory to store filtered data
        predicates_file: File containing predicate URIs (one per line)
        dry_run: If True, only count triples without writing files
    
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
    
    return process_fetched_data(
        input_dir=input_dir,
        output_dir=output_dir,
        allowed_predicates=allowed_predicates,
        dry_run=dry_run
    )


if __name__ == "__main__":
    # Example usage
    INPUT_DIR = "./out"
    OUTPUT_DIR = "./filtered_entity_data"
    
    # Example 1: Filter by specific predicates
    print("=== Filtering by Specific Predicates ===")
    specific_predicates = {
        "http://dbpedia.org/ontology/birthPlace",
        "http://dbpedia.org/ontology/occupation",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    }
    
    stats = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        allowed_predicates=specific_predicates
    )
    
    # Example 2: Filter by namespaces
    print("\n=== Filtering by Namespaces ===")
    namespaces = {
        "http://dbpedia.org/ontology/",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    }
    
    stats2 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_ns",
        allowed_namespaces=namespaces
    )
    
    # Example 3: Combine predicates and namespaces
    print("\n=== Filtering by Predicates AND Namespaces ===")
    combined_predicates = {
        "http://dbpedia.org/ontology/birthPlace",
        "http://dbpedia.org/ontology/occupation"
    }
    combined_namespaces = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "http://www.w3.org/2000/01/rdf-schema#"
    }
    
    stats3 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_combined",
        allowed_predicates=combined_predicates,
        allowed_namespaces=combined_namespaces
    )
    
    # Example 4: Filter by specific types
    print("\n=== Filtering by Specific Types ===")
    allowed_types = {
        "http://dbpedia.org/ontology/Person",
        "http://dbpedia.org/ontology/Film",
        "http://dbpedia.org/ontology/Place"
    }
    
    stats4 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_types",
        allowed_types=allowed_types
    )
    
    # Example 5: Types only (exclude other triples)
    print("\n=== Filtering by Types Only ===")
    stats5 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_types_only",
        allowed_types=allowed_types,
        types_only=True
    )
    
    # Example 6: Combine predicates and types
    print("\n=== Filtering by Predicates AND Types ===")
    stats6 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_pred_types",
        allowed_predicates={"http://dbpedia.org/ontology/birthPlace"},
        allowed_types={"http://dbpedia.org/ontology/Person"}
    )
    
    # Example 7: Replace object URIs with labels
    print("\n=== Replacing Object URIs with Labels ===")
    replace_predicates = {
        "http://dbpedia.org/property/genre",
        "http://dbpedia.org/ontology/occupation",
        "http://dbpedia.org/ontology/birthPlace"
    }
    
    stats7 = process_fetched_data(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_labels",
        allowed_predicates={"http://dbpedia.org/property/genre", "http://dbpedia.org/ontology/occupation"},
        replace_objects=replace_predicates
    )
    
    # Example 8: Use predicates file
    print("\n=== Filtering by Predicates File ===")
    stats8 = construct_from_predicates_file(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR + "_file",
        predicates_file="predicates.txt"
    )
