import hashlib
import re
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
import json

def hash_uri(uri: str) -> str:
    """Hash a URI to create a filename-safe string."""
    return hashlib.md5(uri.encode('utf-8')).hexdigest()

    """
PyOdibel Utilities - Helper functions for working with PyOdibel data.
"""



def filename_to_uri(filename: str) -> Optional[str]:
    """
    Extract the original URI from a PyOdibel filename.
    
    Args:
        filename: The filename (e.g., "dbpedia.org_resource_The_Matrix_work.json")
        
    Returns:
        The original URI or None if the filename doesn't match the expected format
        
    Examples:
        >>> filename_to_uri("dbpedia.org_resource_The_Matrix_work.json")
        'http://dbpedia.org/resource/The_Matrix'
        
        >>> filename_to_uri("dbpedia.org_resource_Keanu_Reeves_person.json")
        'http://dbpedia.org/resource/Keanu_Reeves'
    """
    # Remove .json extension
    if filename.endswith('.json'):
        filename = filename[:-5]
    
    # Check if it matches our pattern
    pattern = r'^dbpedia\.org_resource_(.+)_[a-z]+$'
    match = re.match(pattern, filename)
    
    if match:
        # Extract the resource part and reconstruct the URI
        resource_part = match.group(1)
        # Convert underscores back to spaces and other characters
        resource_part = resource_part.replace('_', ' ')
        return f"http://dbpedia.org/resource/{resource_part}"
    
    return None


def filename_to_uri_and_class(filename: str) -> Optional[Tuple[str, str]]:
    """
    Extract both the original URI and class from a PyOdibel filename.
    
    Args:
        filename: The filename (e.g., "dbpedia.org_resource_The_Matrix_work.json")
        
    Returns:
        Tuple of (uri, class) or None if the filename doesn't match the expected format
        
    Examples:
        >>> filename_to_uri_and_class("dbpedia.org_resource_The_Matrix_work.json")
        ('http://dbpedia.org/resource/The_Matrix', 'work')
        
        >>> filename_to_uri_and_class("dbpedia.org_resource_Keanu_Reeves_person.json")
        ('http://dbpedia.org/resource/Keanu_Reeves', 'person')
    """
    # Remove .json extension
    if filename.endswith('.json'):
        filename = filename[:-5]
    
    # Check if it matches our pattern
    pattern = r'^dbpedia\.org_resource_(.+)_([a-z]+)$'
    match = re.match(pattern, filename)
    
    if match:
        # Extract the resource part and class
        resource_part = match.group(1)
        class_name = match.group(2)
        
        # Convert underscores back to spaces and other characters
        resource_part = resource_part.replace('_', ' ')
        uri = f"http://dbpedia.org/resource/{resource_part}"
        
        return uri, class_name
    
    return None


def uri_to_filename(uri: str, class_name: str) -> str:
    """
    Convert a URI and class to a PyOdibel filename.
    
    Args:
        uri: The original URI
        class_name: The class name (e.g., "dbo:Film" -> "film")
        
    Returns:
        The filename
        
    Examples:
        >>> uri_to_filename("http://dbpedia.org/resource/The_Matrix", "dbo:Film")
        'dbpedia.org_resource_The_Matrix_film.json'
    """
    # Extract resource part from URI
    if uri.startswith('http://dbpedia.org/resource/'):
        resource_part = uri.replace('http://dbpedia.org/resource/', '')
    else:
        resource_part = uri.split('/')[-1]
    
    # Convert to safe filename
    safe_name = resource_part.replace(' ', '_').replace(':', '_')
    
    # Extract class suffix
    if class_name.startswith('dbo:'):
        class_suffix = class_name.replace('dbo:', '').lower()
    else:
        class_suffix = class_name.lower()
    
    return f"dbpedia.org_resource_{safe_name}_{class_suffix}.json"


def load_pyodibel_data(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load data from a PyOdibel JSON file and return both the data and metadata.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary with 'data', 'uri', and 'class' keys, or None if invalid
        
    Examples:
        >>> data = load_pyodibel_data("dbpedia.org_resource_The_Matrix_work.json")
        >>> print(data['uri'])
        'http://dbpedia.org/resource/The_Matrix'
        >>> print(data['class'])
        'work'
    """
    path_obj = Path(file_path)
    
    if not path_obj.exists():
        return None
    
    # Extract URI and class from filename
    uri_info = filename_to_uri_and_class(path_obj.name)
    if not uri_info:
        return None
    
    uri, class_name = uri_info
    
    # Load the JSON data
    try:
        with open(path_obj, 'r') as f:
            data = json.load(f)
        
        return {
            'data': data,
            'uri': uri,
            'class': class_name,
            'filename': path_obj.name
        }
    except (json.JSONDecodeError, IOError):
        return None


def scan_pyodibel_directory(directory: str) -> Dict[str, Dict[str, Any]]:
    """
    Scan a directory for PyOdibel JSON files and return metadata for all found files.
    
    Args:
        directory: Path to the directory to scan
        
    Returns:
        Dictionary mapping URIs to their metadata
        
    Examples:
        >>> files = scan_pyodibel_directory("test_output/")
        >>> for uri, info in files.items():
        ...     print(f"{uri}: {info['class']} ({info['filename']})")
    """
    dir_path = Path(directory)
    if not dir_path.exists():
        return {}
    
    results = {}
    
    for file_path in dir_path.glob("*.json"):
        # Skip the main combined file
        if file_path.name == "fetched_data.json":
            continue
        
        data = load_pyodibel_data(str(file_path))
        if data:
            results[data['uri']] = data
    
    return results


def get_class_statistics(directory: str) -> Dict[str, int]:
    """
    Get statistics about the classes in a PyOdibel output directory.
    
    Args:
        directory: Path to the directory to scan
        
    Returns:
        Dictionary mapping class names to counts
        
    Examples:
        >>> stats = get_class_statistics("test_output/")
        >>> print(stats)
        {'work': 3, 'person': 3}
    """
    files = scan_pyodibel_directory(directory)
    
    class_counts = {}
    for uri, info in files.items():
        class_name = info['class']
        class_counts[class_name] = class_counts.get(class_name, 0) + 1
    
    return class_counts


def main():
    """Example usage of the utility functions."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python util.py <command> [args]")
        print("Commands:")
        print("  extract <filename> - Extract URI from filename")
        print("  scan <directory> - Scan directory for PyOdibel files")
        print("  stats <directory> - Get class statistics")
        return
    
    command = sys.argv[1]
    
    if command == "extract" and len(sys.argv) >= 3:
        filename = sys.argv[2]
        uri_info = filename_to_uri_and_class(filename)
        if uri_info:
            uri, class_name = uri_info
            print(f"URI: {uri}")
            print(f"Class: {class_name}")
        else:
            print("Invalid PyOdibel filename format")
    
    elif command == "scan" and len(sys.argv) >= 3:
        directory = sys.argv[2]
        files = scan_pyodibel_directory(directory)
        print(f"Found {len(files)} PyOdibel files:")
        for uri, info in files.items():
            print(f"  {uri} ({info['class']}) -> {info['filename']}")
    
    elif command == "stats" and len(sys.argv) >= 3:
        directory = sys.argv[2]
        stats = get_class_statistics(directory)
        print("Class statistics:")
        for class_name, count in stats.items():
            print(f"  {class_name}: {count}")
    
    else:
        print("Invalid command or missing arguments")


if __name__ == "__main__":
    main() 