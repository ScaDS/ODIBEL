#!/usr/bin/env python3
"""
Integration test for The Matrix URI fetching.
This test actually connects to DBpedia to verify the fetcher works with real data.
"""

import os
import sys
import tempfile
import yaml
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from acquisition_simple.recursive_fetcher import RecursiveFetcher
from acquisition_simple.config import load_config

def test_matrix_fetch():
    """Test fetching data for The Matrix."""
    print("Testing Matrix URI fetch...")
    
    # Create a simple config for testing
    config_data = {
        "name": "matrix_test",
        "prefixes": [
            {"prefix": "dbo", "uri": "http://dbpedia.org/ontology/"},
            {"prefix": "rdfs", "uri": "http://www.w3.org/2000/01/rdf-schema#"}
        ],
        "ontology": [
            {
                "class": "dbo:Film",
                "properties": [
                    {
                        "property": "dbo:director",
                        "equal": ["wdt:P57"],
                        "range": "dbo:Person"
                    },
                    {
                        "property": "dbo:writer",
                        "equal": ["wdt:P50"],
                        "range": "dbo:Person"
                    },
                    {
                        "property": "dbo:title",
                        "equal": ["wdt:P1476"]
                    }
                ]
            }
        ]
    }
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        yaml.dump(config_data, f)
        temp_path = f.name
    
    try:
        # Initialize fetcher
        fetcher = RecursiveFetcher(config_path=temp_path)
        
        # Fetch The Matrix data
        matrix_uri = "http://dbpedia.org/resource/The_Matrix"
        print(f"Fetching data for: {matrix_uri}")
        
        result = fetcher.fetch_recursively(matrix_uri, max_depth=1)
        
        # Check results
        if matrix_uri in result:
            print("✓ Successfully fetched Matrix data")
            data = result[matrix_uri]
            
            # Check for expected properties
            if "http://dbpedia.org/ontology/director" in data:
                print(f"✓ Found director: {data['http://dbpedia.org/ontology/director']}")
            
            if "http://dbpedia.org/ontology/title" in data:
                print(f"✓ Found title: {data['http://dbpedia.org/ontology/title']}")
            
            # Check summary
            summary = fetcher.get_summary()
            print(f"✓ Total URIs fetched: {summary['total_uris_fetched']}")
            print(f"✓ Visited URIs: {summary['visited_uris']}")
            
            return True
        else:
            print("✗ Failed to fetch Matrix data")
            return False
            
    except Exception as e:
        print(f"✗ Error during fetch: {e}")
        return False
    finally:
        os.unlink(temp_path)

def test_matrix_with_literal_fetch():
    """Test fetching Matrix data with literal property fetching."""
    print("\nTesting Matrix URI with literal fetching...")
    
    # Create config with 'as' field for director
    config_data = {
        "name": "matrix_literal_test",
        "prefixes": [
            {"prefix": "dbo", "uri": "http://dbpedia.org/ontology/"},
            {"prefix": "rdfs", "uri": "http://www.w3.org/2000/01/rdf-schema#"}
        ],
        "ontology": [
            {
                "class": "dbo:Film",
                "properties": [
                    {
                        "property": "dbo:director",
                        "equal": ["wdt:P57"],
                        "range": "dbo:Person",
                        "as": "rdfs:label"  # Fetch director names as literals
                    }
                ]
            }
        ]
    }
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        yaml.dump(config_data, f)
        temp_path = f.name
    
    try:
        # Initialize fetcher
        fetcher = RecursiveFetcher(config_path=temp_path)
        
        # Fetch The Matrix data
        matrix_uri = "http://dbpedia.org/resource/The_Matrix"
        print(f"Fetching data for: {matrix_uri}")
        
        result = fetcher.fetch_recursively(matrix_uri, max_depth=1)
        
        # Check results
        if matrix_uri in result:
            print("✓ Successfully fetched Matrix data with literal processing")
            data = result[matrix_uri]
            
            # Check for director property
            if "http://dbpedia.org/ontology/director" in data:
                directors = data["http://dbpedia.org/ontology/director"]
                print(f"✓ Found directors: {directors}")
                
                # Check if we got literal values (names) instead of URIs
                for director in directors:
                    if not director.startswith("http://"):
                        print(f"✓ Found literal director name: {director}")
                        return True
            
            print("✗ No literal director names found")
            return False
        else:
            print("✗ Failed to fetch Matrix data")
            return False
            
    except Exception as e:
        print(f"✗ Error during fetch: {e}")
        return False
    finally:
        os.unlink(temp_path)

if __name__ == "__main__":
    print("=== Matrix Integration Tests ===\n")
    
    # Run basic fetch test
    success1 = test_matrix_fetch()
    
    # Run literal fetch test
    success2 = test_matrix_with_literal_fetch()
    
    print(f"\n=== Results ===")
    print(f"Basic fetch: {'✓ PASS' if success1 else '✗ FAIL'}")
    print(f"Literal fetch: {'✓ PASS' if success2 else '✗ FAIL'}")
    
    if success1 and success2:
        print("\n🎉 All integration tests passed!")
    else:
        print("\n❌ Some integration tests failed.") 