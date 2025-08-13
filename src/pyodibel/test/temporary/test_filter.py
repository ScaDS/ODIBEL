import os
import json
import tempfile
import shutil
from pathlib import Path
from unittest import TestCase, mock
from rdflib import Graph, URIRef, Literal

from pyodibel.acquisition.filter import (
    parse_ntriples_to_graph,
    filter_graph_by_predicates,
    filter_graph_by_namespaces,
    filter_graph_by_types,
    filter_graph_by_types_only,
    process_fetched_data,
    construct_from_predicates_file,
    load_uri_labels_from_fetched_data
)


class TestFilterModule(TestCase):
    """Test cases for the filter.py module."""

    def setUp(self):
        """Set up test fixtures."""
        # Path to test data
        self.test_data_dir = Path(__file__).parent.parent / "tests_data" / "raw"
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

    def test_filter_graph_by_predicates(self):
        """Test filtering graph by specific predicates."""
        # Create a test graph
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
        
        # Filter by specific predicates
        allowed_predicates = {
            "http://dbpedia.org/property/name",
            "http://dbpedia.org/ontology/starring"
        }
        
        filtered_graph = filter_graph_by_predicates(graph, allowed_predicates)
        
        self.assertEqual(len(filtered_graph), 2)
        self.assertIn((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                      URIRef("http://dbpedia.org/property/name"), 
                      Literal("The Matrix")), filtered_graph)
        self.assertIn((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                      URIRef("http://dbpedia.org/ontology/starring"), 
                      URIRef("http://dbpedia.org/resource/Keanu_Reeves")), filtered_graph)

    def test_filter_graph_by_namespaces(self):
        """Test filtering graph by namespaces."""
        # Create a test graph
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
        self.assertIn((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                      URIRef("http://dbpedia.org/property/name"), 
                      Literal("The Matrix")), filtered_graph)

    def test_filter_graph_by_types(self):
        """Test filtering graph by types."""
        from rdflib import RDF
        
        # Create a test graph
        graph = Graph()
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   RDF.type, 
                   URIRef("http://dbpedia.org/ontology/Film")))
        graph.add((URIRef("http://dbpedia.org/resource/Keanu_Reeves"), 
                   RDF.type, 
                   URIRef("http://dbpedia.org/ontology/Person")))
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/property/name"), 
                   Literal("The Matrix")))
        
        # Filter by specific types
        allowed_types = {
            "http://dbpedia.org/ontology/Film",
            "http://dbpedia.org/ontology/Person"
        }
        
        filtered_graph = filter_graph_by_types(graph, allowed_types)
        
        # Should keep all triples (type triples for allowed types + other triples)
        self.assertEqual(len(filtered_graph), 3)

    def test_filter_graph_by_types_only(self):
        """Test filtering graph by types only (exclude other triples)."""
        from rdflib import RDF
        
        # Create a test graph
        graph = Graph()
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   RDF.type, 
                   URIRef("http://dbpedia.org/ontology/Film")))
        graph.add((URIRef("http://dbpedia.org/resource/Keanu_Reeves"), 
                   RDF.type, 
                   URIRef("http://dbpedia.org/ontology/Person")))
        graph.add((URIRef("http://dbpedia.org/resource/The_Matrix"), 
                   URIRef("http://dbpedia.org/property/name"), 
                   Literal("The Matrix")))
        
        # Filter by specific types only
        allowed_types = {
            "http://dbpedia.org/ontology/Film",
            "http://dbpedia.org/ontology/Person"
        }
        
        filtered_graph = filter_graph_by_types_only(graph, allowed_types)
        
        # Should only keep type triples for allowed types
        self.assertEqual(len(filtered_graph), 2)

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

    def test_filter_rdf_with_predicates_and_types_files(self):
        """Test filtering RDF data using predicates.txt and types.txt files."""
        # Paths to test files
        predicates_file = Path(__file__).parent.parent / "tests_data" / "predicates.txt"
        types_file = Path(__file__).parent.parent / "tests_data" / "types.txt"
        
        # Skip if test data doesn't exist
        if not predicates_file.exists() or not types_file.exists():
            self.skipTest("Test data files not found")
        
        # Read predicates from file
        allowed_predicates = set()
        with open(predicates_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    allowed_predicates.add(line)
        
        # Read types from file
        allowed_types = set()
        with open(types_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    allowed_types.add(line)
        
        print(f"Loaded {len(allowed_predicates)} predicates and {len(allowed_types)} types from files")
        
        # Test with the Matrix entity directory
        if not self.matrix_entity_dir.exists():
            self.skipTest("Matrix test data directory not found")
        
        # Test 1: Filter by predicates only
        print("\n=== Testing predicate filtering ===")
        stats1 = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "predicates_only"),
            allowed_predicates=allowed_predicates,
            dry_run=True
        )
        
        self.assertIsInstance(stats1, dict)
        self.assertIn("files_processed", stats1)
        self.assertIn("total_triples_before", stats1)
        self.assertIn("total_triples_after", stats1)
        self.assertGreaterEqual(stats1["total_triples_after"], 0)
        
        # Test 2: Filter by types only
        print("\n=== Testing type filtering ===")
        stats2 = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "types_only"),
            allowed_types=allowed_types,
            dry_run=True
        )
        
        self.assertIsInstance(stats2, dict)
        self.assertGreaterEqual(stats2["total_triples_after"], 0)
        
        # Test 3: Filter by predicates AND types
        print("\n=== Testing predicate AND type filtering ===")
        stats3 = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "predicates_and_types"),
            allowed_predicates=allowed_predicates,
            allowed_types=allowed_types,
            dry_run=True
        )
        
        self.assertIsInstance(stats3, dict)
        self.assertGreaterEqual(stats3["total_triples_after"], 0)
        
        # Test 4: Filter by types only (exclude other triples)
        print("\n=== Testing types-only filtering ===")
        stats4 = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "types_only_exclusive"),
            allowed_types=allowed_types,
            types_only=True,
            dry_run=True
        )
        
        self.assertIsInstance(stats4, dict)
        self.assertGreaterEqual(stats4["total_triples_after"], 0)
        
        # Test 5: Use construct_from_predicates_file function
        print("\n=== Testing construct_from_predicates_file ===")
        stats5 = construct_from_predicates_file(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "from_predicates_file"),
            predicates_file=str(predicates_file),
            dry_run=True
        )
        
        self.assertIsInstance(stats5, dict)
        self.assertIn("files_processed", stats5)
        
        # Test 6: Actual file generation (not dry run)
        print("\n=== Testing actual file generation ===")
        actual_stats = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "actual_filtered"),
            allowed_predicates=allowed_predicates,
            allowed_types=allowed_types,
            dry_run=False
        )
        
        self.assertIsInstance(actual_stats, dict)
        self.assertGreater(actual_stats["files_processed"], 0)
        
        # Check that output files were created
        output_dir = self.output_dir / "actual_filtered"
        if output_dir.exists():
            data_files = list(output_dir.rglob("data.nt"))
            self.assertGreater(len(data_files), 0)
            
            # Check content of first file
            if data_files:
                with open(data_files[0], 'r') as f:
                    content = f.read()
                    self.assertIsInstance(content, str)
                    # Should contain some filtered triples
                    if content.strip():
                        self.assertIn("http://dbpedia.org", content)
        
        # Test 7: Verify filtering results
        print("\n=== Verifying filtering results ===")
        if output_dir.exists():
            for data_file in output_dir.rglob("data.nt"):
                with open(data_file, 'r') as f:
                    content = f.read()
                
                if content.strip():
                    # Parse the filtered content
                    from rdflib import Graph
                    graph = Graph()
                    graph.parse(data=content, format="ntriples")
                    
                    # Verify that all triples match our filtering criteria
                    for s, p, o in graph:
                        predicate_uri = str(p)
                        
                        # Check if predicate is in our allowed set
                        predicate_match = any(pred in predicate_uri for pred in allowed_predicates)
                        
                        # Check if predicate is from allowed types (rdf:type)
                        from rdflib import RDF
                        type_match = False
                        if p == RDF.type:
                            object_uri = str(o)
                            type_match = any(allowed_type in object_uri for allowed_type in allowed_types)
                        
                        # At least one condition should be met
                        self.assertTrue(
                            predicate_match or type_match,
                            f"Triple {s} {p} {o} does not match filtering criteria"
                        )
        
        print(f"\n=== Filtering Test Summary ===")
        print(f"Predicates loaded: {len(allowed_predicates)}")
        print(f"Types loaded: {len(allowed_types)}")
        print(f"Files processed: {actual_stats.get('files_processed', 0)}")
        print(f"Triples before: {actual_stats.get('total_triples_before', 0)}")
        print(f"Triples after: {actual_stats.get('total_triples_after', 0)}")

    def test_process_fetched_data_with_namespaces(self):
        """Test processing fetched data with namespace filtering."""
        if not self.matrix_entity_dir.exists():
            self.skipTest("Matrix test data directory not found")
        
        # Test namespace filtering
        stats = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "namespace_filtered"),
            allowed_namespaces=self.test_namespaces,
            dry_run=True
        )
        
        self.assertIsInstance(stats, dict)
        self.assertIn("files_processed", stats)
        self.assertIn("total_triples_before", stats)
        self.assertIn("total_triples_after", stats)

    def test_process_fetched_data_validation(self):
        """Test that process_fetched_data validates input parameters."""
        # Test with no filtering criteria
        with self.assertRaises(ValueError):
            process_fetched_data(
                input_dir=str(self.matrix_entity_dir),
                output_dir=str(self.output_dir / "invalid"),
                dry_run=True
            )

    def test_construct_from_predicates_file_missing_file(self):
        """Test construct_from_predicates_file with missing file."""
        missing_file = Path(self.temp_dir) / "missing_predicates.txt"
        
        stats = construct_from_predicates_file(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "missing_file"),
            predicates_file=str(missing_file),
            dry_run=True
        )
        
        # Should return empty stats when file is missing
        self.assertEqual(stats, {})

    def test_filter_with_object_replacement(self):
        """Test filtering with object URI replacement."""
        if not self.matrix_entity_dir.exists():
            self.skipTest("Matrix test data directory not found")
        
        # Test with object replacement
        replace_predicates = {
            "http://dbpedia.org/property/genre",
            "http://dbpedia.org/ontology/starring"
        }
        
        stats = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(self.output_dir / "with_replacement"),
            allowed_predicates={"http://dbpedia.org/property/genre", "http://dbpedia.org/ontology/starring"},
            replace_objects=replace_predicates,
            dry_run=True
        )
        
        self.assertIsInstance(stats, dict)
        self.assertIn("files_processed", stats)

    def test_complete_filter_and_construct_workflow(self):
        """Test the complete workflow: filter RDF data with URI replacement, then construct flat JSON."""
        from pyodibel.acquisition.construct import construct_flat_json_from_filtered_data
        
        # Paths to test files
        predicates_file = Path(__file__).parent.parent / "tests_data" / "predicates.txt"
        types_file = Path(__file__).parent.parent / "tests_data" / "types.txt"
        
        # Skip if test data doesn't exist
        if not predicates_file.exists() or not types_file.exists():
            self.skipTest("Test data files not found")
        
        # Read predicates from file
        allowed_predicates = set()
        with open(predicates_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    allowed_predicates.add(line)
        
        # Read types from file
        allowed_types = set()
        with open(types_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    allowed_types.add(line)
        
        print(f"Loaded {len(allowed_predicates)} predicates and {len(allowed_types)} types from files")
        
        # Test with the Matrix entity directory
        if not self.matrix_entity_dir.exists():
            self.skipTest("Matrix test data directory not found")
        
        # Step 1: Filter the RDF data with comprehensive URI replacement
        print("\n=== Step 1: Filtering RDF data with comprehensive URI replacement ===")
        
        # Define ALL predicates that should have URIs replaced with labels
        replace_predicates = {
            # Ontology predicates
            "http://dbpedia.org/ontology/starring",
            "http://dbpedia.org/ontology/director", 
            "http://dbpedia.org/ontology/producer",
            "http://dbpedia.org/ontology/cinematography",
            "http://dbpedia.org/ontology/musicComposer",
            "http://dbpedia.org/ontology/productionCompany",
            "http://dbpedia.org/ontology/distributor",
            # Property predicates
            "http://dbpedia.org/property/starring",
            "http://dbpedia.org/property/director",
            "http://dbpedia.org/property/writer",
            "http://dbpedia.org/property/producer",
            "http://dbpedia.org/property/cinematography",
            "http://dbpedia.org/property/music",
            "http://dbpedia.org/property/distributor"
        }
        
        filtered_output_dir = self.output_dir / "filtered_with_comprehensive_replacement"
        
        filter_stats = process_fetched_data(
            input_dir=str(self.matrix_entity_dir),
            output_dir=str(filtered_output_dir),
            allowed_predicates=allowed_predicates,
            allowed_types=allowed_types,
            replace_objects=replace_predicates,
            dry_run=False
        )
        
        self.assertIsInstance(filter_stats, dict)
        self.assertGreater(filter_stats["files_processed"], 0)
        self.assertGreater(filter_stats["total_triples_after"], 0)
        
        print(f"Filtering complete: {filter_stats['total_triples_before']} -> {filter_stats['total_triples_after']} triples")
        
        # Step 2: Construct flat JSON from the filtered data
        print("\n=== Step 2: Constructing flat JSON from filtered data ===")
        
        json_output_dir = self.output_dir / "flat_json_output"
        
        flat_json = construct_flat_json_from_filtered_data(
            input_dir=str(filtered_output_dir),
            output_dir=str(json_output_dir),
            dry_run=False
        )
        
        self.assertIsNotNone(flat_json)
        self.assertIsInstance(flat_json, dict)
        
        # Check that the JSON has the expected structure
        if flat_json is not None:
            self.assertIn("_metadata", flat_json)
            self.assertEqual(flat_json["_metadata"]["main_entity"], "http://dbpedia.org/resource/The_Matrix")
            
            # Check that URIs have been replaced with labels/names
            # cinematography should be a string (person name), not a URI
            if "cinematography" in flat_json:
                cinematography_value = flat_json["cinematography"]
                if isinstance(cinematography_value, list):
                    for value in cinematography_value:
                        self.assertIsInstance(value, str)
                        # Should not be a URI
                        self.assertFalse(value.startswith("http://"))
                        print(f"✅ cinematography: {value}")
                else:
                    self.assertIsInstance(cinematography_value, str)
                    self.assertFalse(cinematography_value.startswith("http://"))
                    print(f"✅ cinematography: {cinematography_value}")
            
            # director should be a string or list of strings (not URIs)
            if "director" in flat_json:
                director_value = flat_json["director"]
                if isinstance(director_value, list):
                    for director in director_value:
                        self.assertIsInstance(director, str)
                        self.assertFalse(director.startswith("http://"))
                else:
                    self.assertIsInstance(director_value, str)
                    self.assertFalse(director_value.startswith("http://"))
                print(f"✅ director: {director_value}")
            
            # starring should be a list of strings (not URIs)
            if "starring" in flat_json:
                starring_value = flat_json["starring"]
                if isinstance(starring_value, list):
                    for actor in starring_value:
                        if actor:  # Skip empty strings
                            self.assertIsInstance(actor, str)
                            self.assertFalse(actor.startswith("http://"))
                else:
                    self.assertIsInstance(starring_value, str)
                    self.assertFalse(starring_value.startswith("http://"))
                print(f"✅ starring: {starring_value}")
            
            # Check that the output file was created
            expected_json_file = json_output_dir / "filtered_with_comprehensive_replacement.json"
            self.assertTrue(expected_json_file.exists())
            
            # Read and verify the JSON file content
            with open(expected_json_file, 'r') as f:
                file_content = json.load(f)
            
            self.assertEqual(file_content, flat_json)
            
            print(f"\n=== Complete Workflow Summary ===")
            print(f"Filtered triples: {filter_stats['total_triples_before']} -> {filter_stats['total_triples_after']}")
            print(f"JSON keys: {list(flat_json.keys())}")
            print(f"Output file: {expected_json_file}")
            
            # Show a sample of the flat JSON
            print(f"\n=== Sample Flat JSON ===")
            for key, value in list(flat_json.items())[:5]:  # Show first 5 keys
                if key != "_metadata":
                    print(f"  {key}: {value}")


if __name__ == "__main__":
    import unittest
    unittest.main() 