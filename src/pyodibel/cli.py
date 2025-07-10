#!/usr/bin/env python3
"""
PyOdibel CLI - Command line interface for ontology-driven data acquisition.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Union
from rdflib import Graph

from .acquisition_simple.recursive_fetcher import RecursiveFetcher
from .acquisition_simple.config import load_config


def read_uri_list(uri_file: str) -> List[str]:
    """Read URIs from a file."""
    uris = []
    with open(uri_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                uris.append(line)
    return uris


def save_results(results: Dict[str, Any], output_dir: str, uri_classes: Dict[str, str], base_name: str = "fetched_data", format: str = "json"):
    """Save fetched results to files with class information in filenames."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save main results
    if format == "json":
        main_file = output_path / f"{base_name}.json"
        with open(main_file, 'w') as f:
            json.dump(results, f, indent=2)
    else:
        # RDF format
        main_file = output_path / f"{base_name}.{format}"
        combined_graph = Graph()
        for uri, data in results.items():
            if isinstance(data, Graph):
                combined_graph += data
            else:
                # Convert dictionary back to graph (this would need more complex logic)
                print(f"Warning: Skipping {uri} - dictionary data not supported for RDF output")
        combined_graph.serialize(destination=str(main_file), format=format)
    
    # Save individual URI files with class information
    for uri, data in results.items():
        # Get the class for this URI
        uri_class = uri_classes.get(uri, "unknown")
        
        # Create safe filename from URI and class
        safe_name = uri.replace('http://', '').replace('https://', '').replace('/', '_').replace(':', '_')
        safe_name = safe_name[:80]  # Limit length to leave room for class
        
        # Add class to filename
        class_suffix = uri_class.replace('dbo:', '').lower()  # Convert dbo:Film -> film
        
        if format == "json":
            filename = f"{safe_name}_{class_suffix}.json"
            uri_file = output_path / filename
            
            # Save only the data for this specific URI
            with open(uri_file, 'w') as f:
                json.dump(data, f, indent=2)
        else:
            # RDF format
            filename = f"{safe_name}_{class_suffix}.{format}"
            uri_file = output_path / filename
            
            if isinstance(data, Graph):
                data.serialize(destination=str(uri_file), format=format)
            else:
                print(f"Warning: Skipping {uri} - dictionary data not supported for RDF output")
    
    return main_file


def acq_command(args):
    """Handle the 'acq' subcommand."""
    print(f"Starting acquisition with config: {args.config}")
    print(f"URI list file: {args.uri_file}")
    print(f"Output directory: {args.output}")
    print(f"Max depth: {args.max_depth}")
    
    # Read URI list
    try:
        uris = read_uri_list(args.uri_file)
        print(f"Loaded {len(uris)} URIs from {args.uri_file}")
    except FileNotFoundError:
        print(f"Error: URI file '{args.uri_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading URI file: {e}")
        sys.exit(1)
    
    # Initialize fetcher
    try:
        if args.config:
            fetcher = RecursiveFetcher(config_path=args.config)
        else:
            fetcher = RecursiveFetcher()  # Use default movie config
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)
    
    # Process each URI
    all_results = {}
    uri_classes = {}  # Track the class for each URI
    successful = 0
    failed = 0
    
    for i, uri in enumerate(uris, 1):
        print(f"\nProcessing URI {i}/{len(uris)}: {uri}")
        
        try:
            # Reset fetcher state for each URI
            fetcher.visited_uris.clear()
            fetcher.fetched_data.clear()
            
            # Fetch data
            if args.format == "json":
                result = fetcher.fetch_recursively(uri, max_depth=args.max_depth)
            else:
                # RDF mode - return the filtered graph
                result = fetcher.fetch_recursively(uri, max_depth=args.max_depth, return_graph=True)
            
            if result:
                # Store the data for this specific URI
                all_results[uri] = result
                
                # Get the class for this URI
                class_name = fetcher.get_class_for_uri(uri)
                if class_name:
                    # Extract the class name (e.g., "http://dbpedia.org/ontology/Film" -> "dbo:Film")
                    if "dbpedia.org/ontology/" in class_name:
                        class_short = "dbo:" + class_name.split("/")[-1]
                    else:
                        class_short = class_name
                    uri_classes[uri] = class_short
                    print(f"✓ Identified as: {class_short}")
                else:
                    uri_classes[uri] = "unknown"
                    print(f"⚠ Could not determine class for {uri}")
                
                successful += 1
                print(f"✓ Successfully fetched data for {uri}")
                
                # Show summary
                summary = fetcher.get_summary()
                print(f"  - Fetched {summary['total_uris_fetched']} URIs")
                print(f"  - Visited {len(summary['visited_uris'])} URIs")
            else:
                failed += 1
                print(f"✗ No data fetched for {uri}")
                
        except Exception as e:
            failed += 1
            print(f"✗ Error processing {uri}: {e}")
    
    # Save results
    try:
        main_file = save_results(all_results, args.output, uri_classes, format=args.format)
        print(f"\n✓ Results saved to: {main_file}")
    except Exception as e:
        print(f"✗ Error saving results: {e}")
        sys.exit(1)
    
    # Print summary
    print(f"\n=== Acquisition Summary ===")
    print(f"Total URIs processed: {len(uris)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Results saved to: {args.output}")
    
    # Print class breakdown
    class_counts = {}
    for uri, class_name in uri_classes.items():
        if uri in all_results:  # Only count successful fetches
            class_counts[class_name] = class_counts.get(class_name, 0) + 1
    
    if class_counts:
        print(f"\nClass breakdown:")
        for class_name, count in class_counts.items():
            print(f"  {class_name}: {count}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PyOdibel - Ontology-driven RDF data acquisition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default movie config
  python -m pyodibel.cli acq uris.txt output/

  # Use custom config
  python -m pyodibel.cli acq uris.txt output/ --config my_ontology.conf

  # Set max depth
  python -m pyodibel.cli acq uris.txt output/ --max-depth 2
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # 'acq' subcommand
    acq_parser = subparsers.add_parser(
        'acq',
        help='Acquire RDF data from URIs using ontology configuration'
    )
    
    acq_parser.add_argument(
        'uri_file',
        help='File containing list of URIs (one per line)'
    )
    
    acq_parser.add_argument(
        'output',
        help='Output directory for results'
    )
    
    acq_parser.add_argument(
        '--config', '-c',
        help='Path to ontology configuration file (default: movie.conf)'
    )
    
    acq_parser.add_argument(
        '--max-depth', '-d',
        type=int,
        default=2,
        help='Maximum recursion depth (default: 2)'
    )
    
    acq_parser.add_argument(
        '--format', '-f',
        choices=['json', 'turtle', 'xml', 'nt', 'n3'],
        default='json',
        help='Output format: json, turtle, xml, nt, n3 (default: json)'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Handle subcommands
    if args.command == 'acq':
        acq_command(args)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
