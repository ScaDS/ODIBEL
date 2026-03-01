#!/usr/bin/env python3
"""
Example usage of the URI replacement functionality.

This script demonstrates how to:
1. Create a mapping file
2. Load URI mappings from CSV/TSV
3. Process NT files with URI replacements
"""

import os
import csv
from .replace import load_uri_mapping, process_nt_file, process_nt_directory


def create_example_mapping_file(filename: str = "uri_mapping.csv"):
    """Create an example mapping file."""
    mappings = [
        ["http://example.org/A", "http://mykg.org/A"],
        ["http://example.org/B", "http://mykg.org/B"],
        ["http://dbpedia.org/resource/Berlin", "http://mykg.org/resource/Berlin"],
        ["http://www.wikidata.org/entity/Q64", "http://mykg.org/entity/Q64"],
        ["http://example.org/type", "http://mykg.org/ontology/type"],
        ["http://example.org/name", "http://mykg.org/ontology/name"],
    ]
    
    with open(filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["source_uri", "target_uri"])
        writer.writerows(mappings)
    
    print(f"✅ Created mapping file: {filename}")
    return filename


def create_example_nt_file(filename: str = "example_data.nt"):
    """Create an example NT file with various URIs."""
    content = """# Example NT file with various URIs
<http://example.org/A> <http://example.org/type> <http://example.org/City> .
<http://example.org/B> <http://example.org/name> "Berlin" .
<http://dbpedia.org/resource/Berlin> <http://dbpedia.org/ontology/country> <http://dbpedia.org/resource/Germany> .
<http://www.wikidata.org/entity/Q64> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q183> .
<http://example.org/C> <http://example.org/type> <http://example.org/Person> .
<http://example.org/C> <http://example.org/name> "John Doe" .
"""
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"✅ Created NT file: {filename}")
    return filename


def demonstrate_single_file_processing():
    """Demonstrate processing a single NT file."""
    print("\n" + "="*60)
    print("SINGLE FILE PROCESSING EXAMPLE")
    print("="*60)
    
    # Create example files
    mapping_file = create_example_mapping_file()
    nt_file = create_example_nt_file()
    output_file = "output_single.nt"
    
    try:
        # Load mappings
        uri_mapping = load_uri_mapping(mapping_file)
        print(f"📋 Loaded {len(uri_mapping)} URI mappings")
        
        # Show original content
        print("\n📄 Original NT content:")
        with open(nt_file, 'r', encoding='utf-8') as f:
            print(f.read())
        
        # Process the file
        triples_processed, triples_modified = process_nt_file(nt_file, output_file, uri_mapping)
        
        print(f"\n✅ Processing complete!")
        print(f"   Triples processed: {triples_processed}")
        print(f"   Triples modified: {triples_modified}")
        
        # Show modified content
        print("\n📄 Modified NT content:")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        for file in [mapping_file, nt_file, output_file]:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️  Cleaned up: {file}")


def demonstrate_directory_processing():
    """Demonstrate processing multiple NT files in a directory."""
    print("\n" + "="*60)
    print("DIRECTORY PROCESSING EXAMPLE")
    print("="*60)
    
    # Create example directory structure
    input_dir = "example_input"
    output_dir = "example_output"
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    # Create multiple NT files
    files_content = {
        "file1.nt": """<http://example.org/A> <http://example.org/type> <http://example.org/City> .
<http://example.org/B> <http://example.org/name> "Berlin" .
""",
        "subdir/file2.nt": """<http://dbpedia.org/resource/Berlin> <http://dbpedia.org/ontology/country> <http://dbpedia.org/resource/Germany> .
<http://www.wikidata.org/entity/Q64> <http://www.wikidata.org/prop/direct/P17> <http://www.wikidata.org/entity/Q183> .
""",
        "file3.nt": """<http://example.org/C> <http://example.org/type> <http://example.org/Person> .
<http://example.org/C> <http://example.org/name> "John Doe" .
"""
    }
    
    for filename, content in files_content.items():
        filepath = os.path.join(input_dir, filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Created: {filepath}")
    
    # Create mapping file
    mapping_file = create_example_mapping_file()
    
    try:
        # Load mappings
        uri_mapping = load_uri_mapping(mapping_file)
        print(f"📋 Loaded {len(uri_mapping)} URI mappings")
        
        # Process directory
        files_processed, total_triples, total_modified = process_nt_directory(
            input_dir, output_dir, uri_mapping
        )
        
        print(f"\n✅ Directory processing complete!")
        print(f"   Files processed: {files_processed}")
        print(f"   Total triples: {total_triples}")
        print(f"   Total modified: {total_modified}")
        
        # Show some results
        print("\n📄 Sample output files:")
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                if file.endswith('.nt'):
                    filepath = os.path.join(root, file)
                    print(f"\n--- {filepath} ---")
                    with open(filepath, 'r', encoding='utf-8') as f:
                        print(f.read().strip())
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        import shutil
        for dir_to_remove in [input_dir, output_dir]:
            if os.path.exists(dir_to_remove):
                shutil.rmtree(dir_to_remove)
                print(f"🗑️  Cleaned up directory: {dir_to_remove}")
        
        if os.path.exists(mapping_file):
            os.remove(mapping_file)
            print(f"🗑️  Cleaned up: {mapping_file}")


def demonstrate_tsv_mapping():
    """Demonstrate using TSV format for mappings."""
    print("\n" + "="*60)
    print("TSV MAPPING EXAMPLE")
    print("="*60)
    
    # Create TSV mapping file
    tsv_file = "mapping.tsv"
    mappings = [
        ["http://example.org/A", "http://mykg.org/A"],
        ["http://example.org/B", "http://mykg.org/B"],
    ]
    
    with open(tsv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["source_uri", "target_uri"])
        writer.writerows(mappings)
    
    print(f"✅ Created TSV mapping file: {tsv_file}")
    
    # Create test NT file
    nt_file = "test.nt"
    with open(nt_file, 'w', encoding='utf-8') as f:
        f.write("<http://example.org/A> <http://example.org/type> <http://example.org/City> .\n")
        f.write("<http://example.org/B> <http://example.org/name> \"Berlin\" .\n")
    
    try:
        # Load TSV mappings
        uri_mapping = load_uri_mapping(tsv_file, delimiter='\t')
        print(f"📋 Loaded {len(uri_mapping)} URI mappings from TSV")
        
        # Process file
        output_file = "test_output.nt"
        triples_processed, triples_modified = process_nt_file(nt_file, output_file, uri_mapping)
        
        print(f"✅ Processed with TSV mapping!")
        print(f"   Triples processed: {triples_processed}")
        print(f"   Triples modified: {triples_modified}")
        
        # Show result
        print("\n📄 Modified content:")
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        # Cleanup
        for file in [tsv_file, nt_file, output_file]:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️  Cleaned up: {file}")


if __name__ == "__main__":
    print("🔧 URI Replacement Examples")
    print("This script demonstrates various ways to use the URI replacement functionality.")
    
    # Run examples
    demonstrate_single_file_processing()
    demonstrate_directory_processing()
    demonstrate_tsv_mapping()
    
    print("\n" + "="*60)
    print("✅ All examples completed successfully!")
    print("="*60) 