import csv
import os
import re
from typing import Dict, List, Tuple, Optional
from pathlib import Path


def load_uri_mapping(mapping_file: str, delimiter: Optional[str] = None) -> Dict[str, str]:
    """
    Load URI mappings from a CSV or TSV file.
    
    Args:
        mapping_file: Path to the mapping file (CSV or TSV)
        delimiter: Delimiter to use (None for auto-detection)
    
    Returns:
        Dictionary mapping source URIs to target URIs
    
    Raises:
        FileNotFoundError: If mapping file doesn't exist
        ValueError: If mapping file is malformed
    """
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    
    # Auto-detect delimiter if not specified
    if delimiter is None:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if '\t' in first_line:
                delimiter = '\t'
            elif ',' in first_line:
                delimiter = ','
            else:
                delimiter = '\t'  # Default to tab
    
    uri_mapping = {}
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        
        for row_num, row in enumerate(reader, 1):
            if not row or all(cell.strip() == '' for cell in row):
                continue  # Skip empty rows
            
            if len(row) < 2:
                raise ValueError(f"Invalid mapping format at line {row_num}: expected at least 2 columns")
            
            source_uri = row[0].strip()
            target_uri = row[1].strip()
            
            if source_uri and target_uri:
                uri_mapping[source_uri] = target_uri
    
    return uri_mapping


def replace_uris_in_triple(triple_line: str, uri_mapping: Dict[str, str]) -> str:
    """
    Replace URIs in a single triple line based on the mapping.
    
    Args:
        triple_line: A single NT triple line
        uri_mapping: Dictionary mapping source URIs to target URIs
    
    Returns:
        Modified triple line with URIs replaced
    """
    if not triple_line.strip() or triple_line.startswith('#'):
        return triple_line  # Return unchanged for comments and empty lines
    
    # Pattern to match URIs in angle brackets
    uri_pattern = r'<([^>]+)>'
    
    def replace_uri(match):
        uri = match.group(1)
        return f"<{uri_mapping.get(uri, uri)}>"
    
    return re.sub(uri_pattern, replace_uri, triple_line)


def process_nt_file(input_file: str, output_file: str, uri_mapping: Dict[str, str]) -> Tuple[int, int]:
    """
    Process a single NT file, replacing URIs according to the mapping.
    
    Args:
        input_file: Path to input NT file
        output_file: Path to output NT file
        uri_mapping: Dictionary mapping source URIs to target URIs
    
    Returns:
        Tuple of (triples_processed, triples_modified)
    """
    triples_processed = 0
    triples_modified = 0
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            original_line = line
            modified_line = replace_uris_in_triple(line, uri_mapping)
            
            outfile.write(modified_line)
            triples_processed += 1
            
            if modified_line != original_line:
                triples_modified += 1
    
    return triples_processed, triples_modified


def process_nt_directory(input_dir: str, output_dir: str, uri_mapping: Dict[str, str], 
                        file_pattern: str = "*.nt") -> Tuple[int, int, int]:
    """
    Process all NT files in a directory, replacing URIs according to the mapping.
    
    Args:
        input_dir: Input directory containing NT files
        output_dir: Output directory for processed files
        uri_mapping: Dictionary mapping source URIs to target URIs
        file_pattern: File pattern to match (default: "*.nt")
    
    Returns:
        Tuple of (files_processed, total_triples_processed, total_triples_modified)
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    files_processed = 0
    total_triples_processed = 0
    total_triples_modified = 0
    
    # Find all matching files
    for nt_file in input_path.rglob(file_pattern):
        # Calculate relative path to maintain directory structure
        rel_path = nt_file.relative_to(input_path)
        output_file = output_path / rel_path
        
        try:
            triples_processed, triples_modified = process_nt_file(
                str(nt_file), str(output_file), uri_mapping
            )
            
            files_processed += 1
            total_triples_processed += triples_processed
            total_triples_modified += triples_modified
            
            print(f"✅ Processed: {nt_file} -> {output_file}")
            print(f"   Triples: {triples_processed} processed, {triples_modified} modified")
            
        except Exception as e:
            print(f"❌ Failed to process {nt_file}: {e}")
    
    return files_processed, total_triples_processed, total_triples_modified


def create_sample_mapping_file(output_file: str = "sample_uri_mapping.csv") -> None:
    """
    Create a sample mapping file for demonstration purposes.
    
    Args:
        output_file: Path to the sample mapping file
    """
    sample_mappings = [
        ["http://example.org/A", "http://mykg.org/A"],
        ["http://example.org/B", "http://mykg.org/B"],
        ["http://dbpedia.org/resource/Berlin", "http://mykg.org/resource/Berlin"],
        ["http://www.wikidata.org/entity/Q64", "http://mykg.org/entity/Q64"],
    ]
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["source_uri", "target_uri"])  # Header
        writer.writerows(sample_mappings)
    
    print(f"📝 Sample mapping file created: {output_file}")


def main():
    """
    Main function demonstrating usage of the URI replacement functionality.
    """
    # Example usage
    print("🔧 URI Replacement Tool")
    print("=" * 50)
    
    # Create sample mapping file
    mapping_file = "sample_uri_mapping.csv"
    create_sample_mapping_file(mapping_file)
    
    # Load URI mappings
    try:
        uri_mapping = load_uri_mapping(mapping_file)
        print(f"📋 Loaded {len(uri_mapping)} URI mappings")
        
        # Example mappings
        for source, target in list(uri_mapping.items())[:3]:
            print(f"   {source} -> {target}")
        
    except Exception as e:
        print(f"❌ Error loading mapping file: {e}")
        return
    
    # Example: Process a single file
    input_file = "example.nt"
    output_file = "example_modified.nt"
    
    # Create a sample NT file for testing
    sample_nt_content = """<http://example.org/A> <http://example.org/type> <http://example.org/City> .
<http://example.org/B> <http://example.org/name> "Berlin" .
<http://dbpedia.org/resource/Berlin> <http://dbpedia.org/ontology/country> <http://dbpedia.org/resource/Germany> .
"""
    
    with open(input_file, 'w', encoding='utf-8') as f:
        f.write(sample_nt_content)
    
    print(f"\n📁 Created sample NT file: {input_file}")
    
    # Process the file
    try:
        triples_processed, triples_modified = process_nt_file(input_file, output_file, uri_mapping)
        print(f"✅ Processed {input_file} -> {output_file}")
        print(f"   Triples: {triples_processed} processed, {triples_modified} modified")
        
        # Show the result
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f"\n📄 Modified content:")
            print(f.read())
            
    except Exception as e:
        print(f"❌ Error processing file: {e}")
    
    # Cleanup sample files
    for file in [mapping_file, input_file, output_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️  Cleaned up: {file}")


if __name__ == "__main__":
    main()
