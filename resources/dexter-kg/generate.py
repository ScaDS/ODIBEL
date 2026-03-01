"""
Load Gradoop/FAMER export data using GradoopDataSource.

This script demonstrates loading entity resolution results from a Gradoop export folder
using the new entity-based API and generating RDF graphs per source.
"""

from pathlib import Path
from urllib.parse import quote
from typing import Dict, List

from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS

from pyodibel.api.data_source import DataSourceConfig
from pyodibel.source.gradoop.source import GradoopDataSource
from pyodibel.source.gradoop.entity import GradoopEntity

if __name__ == "__main__":
    # Create data source with default path to the Gradoop export
    source = GradoopDataSource(
        DataSourceConfig(name="ds-c0"),
        default_path="/d/repo/MA Lerm/Datensammlungen/DS-C/DS-C0/SW_0.3",
        default_format="csv"
    )
    
    # Fetch entities using the new entity-based API
    print("Loading Gradoop data...")
    entities = list(source.fetch())
    
    # Display summary
    print(f"\nLoaded data summary:")
    print(f"  Entities: {len(entities)}")
    
    # Group by source
    sources = {}
    for entity in entities:
        if entity.resource not in sources:
            sources[entity.resource] = []
        sources[entity.resource].append(entity)
    
    print(f"  Sources: {len(sources)}")
    for source_name, source_entities in sources.items():
        print(f"    - {source_name}: {len(source_entities)} entities")
    
    # Display example entities with their properties
    if entities:
        print(f"\nExample entities (first 5):")
        for i, entity in enumerate(entities[:5]):
            print(f"  {i+1}. {entity}")
            print(f"     IRI: {entity.iri}")
            print(f"     Resource: {entity.resource}")
            if entity.cluster_id:
                print(f"     Cluster ID: {entity.cluster_id}")
            print(f"     Properties: {len(entity.properties)}")
            # Show first few property keys
            if entity.properties:
                prop_keys = list(entity.properties.keys())[:3]
                print(f"     Sample properties: {', '.join(prop_keys)}")
            print()
    
    # Demonstrate entity API usage
    if entities:
        example = entities[0]
        print(f"\nEntity API demonstration:")
        print(f"  Entity ID: {example.id}")
        print(f"  Source: {example.source}")
        print(f"  Get property: {example.get_property('recId', 'N/A')}")
        print(f"  Has property 'recId': {example.has_property('recId')}")
        print(f"  All properties count: {len(example.get_all_properties())}")
        print(f"  To dict: {list(example.to_dict().keys())}")
    
    print("\nData loaded successfully!")
    print("Entities are now GradoopEntity objects with a clean, type-safe API.")
    
    # Generate RDF graphs for each source
    print("\n" + "="*60)
    print("Generating RDF graphs per source...")
    print("="*60)
    
    output_dir = Path("output/rdf")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define namespaces
    KG = Namespace("http://kg.org/ontology/")
    ENTITY = Namespace("http://kg.org/entity/")
    
    graphs_by_source = {}
    
    for source_name, source_entities in sources.items():
        print(f"\nProcessing source: {source_name} ({len(source_entities)} entities)")
        
        # Create a new graph for this source
        graph = Graph()
        graph.bind("kg", KG)
        graph.bind("entity", ENTITY)
        graph.bind("rdf", RDF)
        graph.bind("rdfs", RDFS)
        
        # Sanitize source name for file naming
        safe_source_name = quote(source_name, safe='').replace('/', '_').replace(':', '_')
        
        entity_count = 0
        for entity in source_entities:
            # Create entity URI
            entity_uri = URIRef(f"{ENTITY}{entity.iri}")
            
            # Add entity type (we can use a generic type or derive from properties)
            graph.add((entity_uri, RDF.type, KG.Entity))
            
            # Add source information
            graph.add((entity_uri, KG.hasSource, Literal(entity.resource)))
            
            # Add cluster information if available
            if entity.cluster_id:
                graph.add((entity_uri, KG.belongsToCluster, Literal(entity.cluster_id)))
            
            # Convert properties to RDF triples
            for prop_key, prop_value in entity.properties.items():
                if prop_value is None or prop_value == "":
                    continue
                
                # Create predicate URI (sanitize property key)
                # Remove angle brackets and other special chars
                clean_key = prop_key.strip('<>').replace(' ', '_').replace('-', '_')
                predicate = URIRef(f"{KG}{clean_key}")
                
                # Convert value to appropriate RDF type
                if isinstance(prop_value, (int, float)):
                    obj = Literal(prop_value)
                elif isinstance(prop_value, bool):
                    obj = Literal(prop_value, datatype=RDF.XMLLiteral)
                elif isinstance(prop_value, str):
                    # Check if it looks like a URI
                    if prop_value.startswith(('http://', 'https://')):
                        obj = URIRef(prop_value)
                    else:
                        obj = Literal(prop_value)
                else:
                    obj = Literal(str(prop_value))
                
                graph.add((entity_uri, predicate, obj))
            
            entity_count += 1
        
        graphs_by_source[source_name] = graph
        
        # Save graph to file
        output_file = output_dir / f"{safe_source_name}.ttl"
        graph.serialize(destination=str(output_file), format="turtle")
        print(f"  ✓ Saved {entity_count} entities to {output_file}")
        print(f"  ✓ Graph contains {len(graph)} triples")
    
    # Also create a combined graph with all sources
    print(f"\nCreating combined graph with all sources...")
    combined_graph = Graph()
    combined_graph.bind("kg", KG)
    combined_graph.bind("entity", ENTITY)
    combined_graph.bind("rdf", RDF)
    combined_graph.bind("rdfs", RDFS)
    
    for graph in graphs_by_source.values():
        for triple in graph:
            combined_graph.add(triple)
    
    combined_file = output_dir / "all_sources.ttl"
    combined_graph.serialize(destination=str(combined_file), format="turtle")
    print(f"  ✓ Combined graph saved to {combined_file}")
    print(f"  ✓ Combined graph contains {len(combined_graph)} triples")
    
    print(f"\n{'='*60}")
    print(f"RDF generation complete!")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"Generated {len(graphs_by_source)} source graphs + 1 combined graph")
    print(f"{'='*60}")