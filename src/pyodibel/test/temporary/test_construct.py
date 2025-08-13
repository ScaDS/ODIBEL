import os
import json
import tempfile
import shutil
from pathlib import Path
from unittest import TestCase, mock
from rdflib import Graph, URIRef

from pyodibel.acquisition.construct import (
    build_recursive_json,
    build_comprehensive_json,
    filter_graph_by_namespaces,
    parse_ntriples_to_graph,
    load_uri_labels_from_fetched_data,
    construct_json_source,
    construct_json_from_predicates_file
)


class TestConstructModule(TestCase):
    """Test cases for the construct.py module."""

    def setUp(self):
        """Set up test fixtures."""
        # Path to test data
        self.test_data_dir = Path(__file__).parent / "tests_data" / "filter"
        self.matrix_entity_dir = self.test_data_dir / "58b4456d6ff046f1482938342bb28391"
        
        # Create temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "test_output"
        
        # Test namespaces
        self.test_namespaces = {
            "http://dbpedia.org/property/",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#"
        }

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_parse_ntriples_to_graph(self):
        """Test parsing N-Triples data into a graph."""
        # Test with valid N-Triples data
        ntriples_data = """
        <http://dbpedia.org/resource/The_Matrix> <http://dbpedia.org/property/name> "The Matrix"@en .
        <http://dbpedia.org/resource/The_Matrix> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Film> .
        """
        
        graph = parse_ntriples_to_graph(ntriples_data)
        
        self.assertIsInstance(graph, Graph)
        self.assertEqual(len(graph), 2)
        
        # Test with empty data
        empty_graph = parse_ntriples_to_graph("")
        self.assertEqual(len(empty_graph), 0)
        
        # Test with invalid data
        invalid_graph = parse_ntriples_to_graph("invalid n-triples data")
        self.assertEqual(len(invalid_graph), 0)

    def test_filter_graph_by_namespaces(self):
        """Test filtering graph by namespaces."""
        # Create a test graph
        from rdflib import Literal
        graph = Graph()
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/property/name"), 
                   Literal("The Matrix")))
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/ontology/starring"), 
                   URIRef("http://dbpedia.org/resource/Keanu_Reeves")))
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://other.org/property/test"), 
                   Literal("test")))
        
        # Filter by dbpedia.org/property namespace
        filtered_graph = filter_graph_by_namespaces(graph, {"http://dbpedia.org/property/"})
        
        self.assertEqual(len(filtered_graph), 1)
        # self.assertIn((URIRef("http://dbpedia.org/resource/The_Matrix"), 
        #               URIRef("http://dbpedia.org/property/name"), 
        #               URIRef("The Matrix")), filtered_graph)

    def test_build_recursive_json(self):
        """Test building recursive JSON representation."""
        # Create a simple test graph
        from rdflib import Literal
        graph = Graph()
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/property/name"), 
                   Literal("The Matrix")))
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/property/starring"), 
                   URIRef("http://dbpedia.org/resource/Keanu_Reeves")))
        graph.add((URIRef("http://dbpedia.org/resource/Keanu_Reeves"), 
                   URIRef("http://dbpedia.org/property/name"), 
                   Literal("Keanu Reeves")))
        
        # Build JSON for The Matrix
        json_data = build_recursive_json("http://dbpedia.org/resource/The_Matrix", graph, max_depth=2)
        
        self.assertIsNotNone(json_data)
        self.assertIsInstance(json_data, dict)
        if json_data is not None:
            self.assertIn("name", json_data)
            self.assertEqual(json_data["name"], "The Matrix")
            self.assertIn("starring", json_data)
            self.assertIn("provenance_hash", json_data)
        
        # Test cycle detection
        # Create a graph with a cycle
        cycle_graph = Graph()
        cycle_graph.add((URIRef("http://dbpedia.org/resource/A"), 
                        URIRef("http://dbpedia.org/property/refers"), 
                        URIRef("http://dbpedia.org/resource/B")))
        cycle_graph.add((URIRef("http://dbpedia.org/resource/B"), 
                        URIRef("http://dbpedia.org/property/refers"), 
                        URIRef("http://dbpedia.org/resource/A")))
        
        cycle_json = build_recursive_json("http://dbpedia.org/resource/A", cycle_graph, max_depth=3)
        self.assertIsNotNone(cycle_json)  # Should handle cycles gracefully

    def test_load_uri_labels_from_fetched_data(self):
        """Test loading URI labels from fetched data."""
        # This test requires the actual test data directory
        if self.matrix_entity_dir.exists():
            uri_labels = load_uri_labels_from_fetched_data(str(self.matrix_entity_dir))
            
            self.assertIsInstance(uri_labels, dict)
            # Should have loaded some labels
            self.assertGreater(len(uri_labels), 0)
            
            # Check for specific expected labels
            matrix_uri = "http://dbpedia.org/resource/The_Matrix"
            if matrix_uri in uri_labels:
                self.assertEqual(uri_labels[matrix_uri], "The Matrix")

    def test_build_comprehensive_json(self):
        """Test building comprehensive JSON with nested entities."""
        # This test requires the actual test data directory
        if not self.matrix_entity_dir.exists():
            self.skipTest("Test data directory not found")
        
        # Test with the Matrix entity
        json_data = build_comprehensive_json(
            "http://dbpedia.org/resource/The_Matrix",
            self.matrix_entity_dir,
            self.test_namespaces,
            max_depth=2,
            uri_labels={}
        )
        
        self.assertIsNotNone(json_data)
        self.assertIsInstance(json_data, dict)
        if json_data is not None:
            self.assertIn("_metadata", json_data)
            self.assertEqual(json_data["_metadata"]["main_entity"], "http://dbpedia.org/resource/The_Matrix")
            self.assertIn("nested_entities_count", json_data["_metadata"])
            self.assertIn("total_triples", json_data["_metadata"])
            self.assertIn("filtered_namespaces", json_data["_metadata"])

    def test_construct_json_source(self):
        """Test the main construct_json_source function."""
        # This test requires the actual test data directory
        if not self.matrix_entity_dir.exists():
            self.skipTest("Test data directory not found")
        
        # Test with dry run first
        stats = construct_json_source(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            allowed_namespaces=self.test_namespaces,
            max_depth=2,
            dry_run=True
        )
        
        self.assertIsInstance(stats, dict)
        self.assertIn("files_processed", stats)
        self.assertIn("entities_with_data", stats)
        self.assertIn("json_files_created", stats)
        self.assertIn("total_triples_before", stats)
        self.assertIn("total_triples_after", stats)
        
        # Test actual file creation
        stats = construct_json_source(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            allowed_namespaces=self.test_namespaces,
            max_depth=2,
            dry_run=False
        )
        
        # Check that output file was created
        expected_file = self.output_dir / "58b4456d6ff046f1482938342bb28391.json"
        self.assertTrue(expected_file.exists())
        
        # Check the JSON content
        with open(expected_file, 'r') as f:
            json_data = json.load(f)
        
        self.assertIsInstance(json_data, dict)
        if json_data is not None:
            self.assertIn("_metadata", json_data)
            self.assertEqual(json_data["_metadata"]["main_entity"], "http://dbpedia.org/resource/The_Matrix")

    def test_construct_json_source_with_custom_namespaces(self):
        """Test construct_json_source with custom namespaces."""
        if not self.matrix_entity_dir.exists():
            self.skipTest("Test data directory not found")
        
        custom_namespaces = {
            "http://dbpedia.org/property/",
            "http://dbpedia.org/ontology/"
        }
        
        stats = construct_json_source(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            allowed_namespaces=custom_namespaces,
            max_depth=1,
            dry_run=True
        )
        
        self.assertIsInstance(stats, dict)
        self.assertGreater(stats["files_processed"], 0)

    def test_construct_json_from_predicates_file(self):
        """Test construct_json_from_predicates_file function."""
        if not self.matrix_entity_dir.exists():
            self.skipTest("Test data directory not found")
        
        # Create a temporary predicates file
        predicates_file = Path(self.temp_dir) / "test_predicates.txt"
        with open(predicates_file, 'w') as f:
            f.write("http://dbpedia.org/property/name\n")
            f.write("http://dbpedia.org/property/starring\n")
            f.write("http://www.w3.org/1999/02/22-rdf-syntax-ns#type\n")
        
        stats = construct_json_from_predicates_file(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            predicates_file=str(predicates_file),
            max_depth=2,
            dry_run=True
        )
        
        self.assertIsInstance(stats, dict)
        self.assertGreater(stats["files_processed"], 0)

    def test_construct_json_from_predicates_file_missing_file(self):
        """Test construct_json_from_predicates_file with missing file."""
        stats = construct_json_from_predicates_file(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            predicates_file="nonexistent_file.txt",
            max_depth=2,
            dry_run=True
        )
        
        self.assertEqual(stats, {})

    def test_construct_json_source_empty_directory(self):
        """Test construct_json_source with empty directory."""
        empty_dir = Path(self.temp_dir) / "empty_dir"
        empty_dir.mkdir()
        
        stats = construct_json_source(
            input_dir=str(empty_dir),
            output_dir=str(self.output_dir),
            allowed_namespaces=self.test_namespaces,
            max_depth=2,
            dry_run=True
        )
        
        self.assertEqual(stats["files_processed"], 0)
        self.assertEqual(stats["entities_with_data"], 0)

    def test_construct_json_source_invalid_directory(self):
        """Test construct_json_source with invalid directory."""
        stats = construct_json_source(
            input_dir="nonexistent_directory",
            output_dir=str(self.output_dir),
            allowed_namespaces=self.test_namespaces,
            max_depth=2,
            dry_run=True
        )
        
        self.assertEqual(stats["files_processed"], 0)

    def test_build_recursive_json_max_depth(self):
        """Test build_recursive_json with max depth limit."""
        # Create a deep graph
        graph = Graph()
        graph.add((URIRef("http://dbpedia.org/resource/A"), 
                   URIRef("http://dbpedia.org/property/refers"), 
                   URIRef("http://dbpedia.org/resource/B")))
        graph.add((URIRef("http://dbpedia.org/resource/B"), 
                   URIRef("http://dbpedia.org/property/refers"), 
                   URIRef("http://dbpedia.org/resource/C")))
        graph.add((URIRef("http://dbpedia.org/resource/C"), 
                   URIRef("http://dbpedia.org/property/refers"), 
                   URIRef("http://dbpedia.org/resource/D")))
        
        # Test with max_depth=1
        json_data = build_recursive_json("http://dbpedia.org/resource/A", graph, max_depth=1)
        
        self.assertIsInstance(json_data, dict)
        self.assertIn("refers", json_data)
        # Should not have nested data beyond depth 1
        self.assertIsInstance(json_data["refers"], str)

    def test_json_output_structure(self):
        """Test that the JSON output has the expected structure."""
        if not self.matrix_entity_dir.exists():
            self.skipTest("Test data directory not found")
        
        # Create JSON output
        stats = construct_json_source(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir),
            allowed_namespaces=self.test_namespaces,
            max_depth=2,
            dry_run=False
        )
        
        # Check the output file
        expected_file = self.output_dir / "58b4456d6ff046f1482938342bb28391.json"
        self.assertTrue(expected_file.exists())
        
        with open(expected_file, 'r') as f:
            json_data = json.load(f)
        
        # Check basic structure
        self.assertIn("_metadata", json_data)
        self.assertIn("provenance_hash", json_data)
        
        # Check metadata structure
        metadata = json_data["_metadata"]
        self.assertIn("main_entity", metadata)
        self.assertIn("nested_entities_count", metadata)
        self.assertIn("total_triples", metadata)
        self.assertIn("filtered_namespaces", metadata)
        
        # Check that values are strings (not nested objects with "value" keys)
        if json_data is not None:
            for key, value in json_data.items():
                if key not in ["_metadata", "provenance_hash"]:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                # Nested objects should not have "value" wrapper
                                self.assertNotIn("value", item)
                    elif isinstance(value, dict):
                        # Nested objects should not have "value" wrapper
                        self.assertNotIn("value", value)




if __name__ == "__main__":
    import unittest
    unittest.main() 