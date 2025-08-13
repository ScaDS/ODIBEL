import unittest
import os
import tempfile
import yaml
from unittest.mock import patch, Mock
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from acquisition_simple.recursive_fetcher import RecursiveFetcher
from acquisition_simple.config import load_config, OntologyConfigModel

class TestRecursiveFetcherSimple(unittest.TestCase):
    """Simple test cases for the RecursiveFetcher class using unittest."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_config = {
            "name": "test",
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
                        }
                    ]
                },
                {
                    "class": "dbo:Person",
                    "properties": [
                        {
                            "property": "dbo:birthPlace",
                            "equal": ["wdt:P19"],
                            "range": "dbo:Place",
                            "as": "rdfs:label"
                        }
                    ]
                }
            ]
        }
    
    def test_fetcher_initialization(self):
        """Test that the fetcher initializes correctly."""
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(self.sample_config, f)
            temp_path = f.name
        
        try:
            fetcher = RecursiveFetcher(config_path=temp_path)
            self.assertIsNotNone(fetcher.config)
            self.assertEqual(fetcher.config.name, "test")
            self.assertEqual(len(fetcher.config.ontology), 2)
        finally:
            os.unlink(temp_path)
    
    def test_get_property_config(self):
        """Test getting property configuration for a class."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(self.sample_config, f)
            temp_path = f.name
        
        try:
            fetcher = RecursiveFetcher(config_path=temp_path)
            
            # Test existing property
            prop_config = fetcher.get_property_config("dbo:Film", "dbo:director")
            self.assertIsNotNone(prop_config)
            if prop_config:
                self.assertEqual(prop_config.property, "dbo:director")
                self.assertEqual(prop_config.range, "dbo:Person")
            
            # Test non-existing property
            prop_config = fetcher.get_property_config("dbo:Film", "dbo:nonExistent")
            self.assertIsNone(prop_config)
        finally:
            os.unlink(temp_path)
    
    def test_get_followable_properties(self):
        """Test getting followable properties for a class."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(self.sample_config, f)
            temp_path = f.name
        
        try:
            fetcher = RecursiveFetcher(config_path=temp_path)
            
            # Film should have director and writer as followable properties
            followable = fetcher.get_followable_properties("dbo:Film")
            self.assertIn("dbo:director", followable)
            self.assertIn("dbo:writer", followable)
            
            # Person should have birthPlace as followable property
            followable = fetcher.get_followable_properties("dbo:Person")
            self.assertIn("dbo:birthPlace", followable)
        finally:
            os.unlink(temp_path)
    
    @patch('requests.get')
    def test_get_class_for_uri(self, mock_get):
        """Test getting the class of a URI."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {"type": {"value": "http://dbpedia.org/ontology/Film"}}
                ]
            }
        }
        mock_get.return_value = mock_response
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(self.sample_config, f)
            temp_path = f.name
        
        try:
            fetcher = RecursiveFetcher(config_path=temp_path)
            class_name = fetcher.get_class_for_uri("http://dbpedia.org/resource/The_Matrix")
            self.assertEqual(class_name, "http://dbpedia.org/ontology/Film")
        finally:
            os.unlink(temp_path)
    
    def test_is_followable_uri(self):
        """Test checking if a URI should be followed."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(self.sample_config, f)
            temp_path = f.name
        
        try:
            fetcher = RecursiveFetcher(config_path=temp_path)
            
            # Test followable URI - director is in followable properties for Film
            is_followable = fetcher.is_followable_uri(
                "http://dbpedia.org/resource/Lana_Wachowski",
                "dbo:director",  # Use the property name, not full URI
                "dbo:Film"
            )
            self.assertTrue(is_followable)
            
            # Test non-followable URI (external)
            is_followable = fetcher.is_followable_uri(
                "http://example.com/resource",
                "dbo:director",
                "dbo:Film"
            )
            self.assertFalse(is_followable)
        finally:
            os.unlink(temp_path)
    
    def test_fetcher_with_preloaded_config(self):
        """Test fetcher with pre-loaded configuration."""
        config = OntologyConfigModel(**self.sample_config)
        fetcher = RecursiveFetcher(config=config)
        
        self.assertEqual(fetcher.config.name, "test")
        self.assertEqual(len(fetcher.config.ontology), 2)
    
    @patch('acquisition_simple.config.get_movie_ontology')
    def test_fetcher_default_initialization(self, mock_get_movie_ontology):
        """Test fetcher with default initialization (uses movie config)."""
        # Mock the movie ontology to avoid file path issues
        mock_config = OntologyConfigModel(**self.sample_config)
        mock_get_movie_ontology.return_value = mock_config
        
        fetcher = RecursiveFetcher()
        
        self.assertIsNotNone(fetcher.config)
        self.assertEqual(fetcher.config.name, "test")  # Using our mock config

def run_tests():
    """Run the tests."""
    print("Running RecursiveFetcher tests...")
    unittest.main(verbosity=2)

if __name__ == "__main__":
    run_tests() 