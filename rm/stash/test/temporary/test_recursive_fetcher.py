import pytest
import os
import tempfile
import yaml
from unittest.mock import patch, Mock
from ..acquisition_simple.recursive_fetcher import RecursiveFetcher
from ..acquisition_simple.config import load_config, OntologyConfigModel

class TestRecursiveFetcher:
    """Test cases for the RecursiveFetcher class."""
    
    @pytest.fixture
    def sample_config(self):
        """Create a sample configuration for testing."""
        config_data = {
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
        return config_data
    
    @pytest.fixture
    def temp_config_file(self, sample_config):
        """Create a temporary config file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            yaml.dump(sample_config, f)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        os.unlink(temp_path)
    
    def test_fetcher_initialization(self, temp_config_file):
        """Test that the fetcher initializes correctly with a config file."""
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        assert fetcher.config is not None
        assert fetcher.config.name == "test"
        assert len(fetcher.config.ontology) == 2
    
    def test_get_property_config(self, temp_config_file):
        """Test getting property configuration for a class."""
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        
        # Test existing property
        prop_config = fetcher.get_property_config("dbo:Film", "dbo:director")
        assert prop_config is not None
        assert prop_config.property == "dbo:director"
        assert prop_config.range == "dbo:Person"
        
        # Test non-existing property
        prop_config = fetcher.get_property_config("dbo:Film", "dbo:nonExistent")
        assert prop_config is None
    
    def test_get_followable_properties(self, temp_config_file):
        """Test getting followable properties for a class."""
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        
        # Film should have director and writer as followable properties
        followable = fetcher.get_followable_properties("dbo:Film")
        assert "dbo:director" in followable
        assert "dbo:writer" in followable
        
        # Person should have birthPlace as followable property
        followable = fetcher.get_followable_properties("dbo:Person")
        assert "dbo:birthPlace" in followable
    
    @patch('requests.get')
    def test_get_class_for_uri(self, mock_get, temp_config_file):
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
        
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        class_name = fetcher.get_class_for_uri("http://dbpedia.org/resource/The_Matrix")
        
        assert class_name == "http://dbpedia.org/ontology/Film"
    
    @patch('requests.get')
    def test_fetch_specific_property(self, mock_get, temp_config_file):
        """Test fetching a specific property value."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {"value": {"value": "Los Angeles"}}
                ]
            }
        }
        mock_get.return_value = mock_response
        
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        value = fetcher.fetch_specific_property(
            "http://dbpedia.org/resource/Los_Angeles",
            "http://www.w3.org/2000/01/rdf-schema#label"
        )
        
        assert value == "Los Angeles"
    
    @patch('requests.get')
    def test_fetch_uri_data(self, mock_get, temp_config_file):
        """Test fetching all data for a URI."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {
                        "p": {"value": "http://dbpedia.org/ontology/director"},
                        "o": {"value": "http://dbpedia.org/resource/Lana_Wachowski"}
                    },
                    {
                        "p": {"value": "http://dbpedia.org/ontology/title"},
                        "o": {"value": "The Matrix"}
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        data = fetcher.fetch_uri_data("http://dbpedia.org/resource/The_Matrix")
        
        assert "http://dbpedia.org/ontology/director" in data
        assert "http://dbpedia.org/ontology/title" in data
        assert "http://dbpedia.org/resource/Lana_Wachowski" in data["http://dbpedia.org/ontology/director"]
        assert "The Matrix" in data["http://dbpedia.org/ontology/title"]
    
    def test_is_followable_uri(self, temp_config_file):
        """Test checking if a URI should be followed."""
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        
        # Test followable URI
        is_followable = fetcher.is_followable_uri(
            "http://dbpedia.org/resource/Lana_Wachowski",
            "http://dbpedia.org/ontology/director",
            "dbo:Film"
        )
        assert is_followable is True
        
        # Test non-followable URI (external)
        is_followable = fetcher.is_followable_uri(
            "http://example.com/resource",
            "http://dbpedia.org/ontology/director",
            "dbo:Film"
        )
        assert is_followable is False
    
    @patch('requests.get')
    def test_process_property_values_with_as_field(self, mock_get, temp_config_file):
        """Test processing property values when 'as' field is specified."""
        # Mock responses for specific property fetching
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": {
                "bindings": [
                    {"value": {"value": "Los Angeles"}}
                ]
            }
        }
        mock_get.return_value = mock_response
        
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        
        # Test with 'as' field - should fetch rdfs:label instead of URI
        objects = ["http://dbpedia.org/resource/Los_Angeles"]
        processed = fetcher.process_property_values(
            "http://dbpedia.org/resource/Keanu_Reeves",
            "dbo:Person",
            "dbo:birthPlace",
            objects
        )
        
        # Should return the label instead of URI
        assert processed == ["Los Angeles"]
    
    @patch('requests.get')
    def test_fetch_recursively_matrix(self, mock_get, temp_config_file):
        """Test recursive fetching starting from The Matrix URI."""
        # Mock responses for different queries
        def mock_response_side_effect(*args, **kwargs):
            mock_response = Mock()
            mock_response.status_code = 200
            
            # Determine which query based on parameters
            query = kwargs.get('params', {}).get('query', '')
            
            if 'rdf:type' in query:
                # Class query
                mock_response.json.return_value = {
                    "results": {
                        "bindings": [
                            {"type": {"value": "http://dbpedia.org/ontology/Film"}}
                        ]
                    }
                }
            elif 'rdfs:label' in query:
                # Specific property query
                mock_response.json.return_value = {
                    "results": {
                        "bindings": [
                            {"value": {"value": "Lana Wachowski"}}
                        ]
                    }
                }
            else:
                # General data query
                mock_response.json.return_value = {
                    "results": {
                        "bindings": [
                            {
                                "p": {"value": "http://dbpedia.org/ontology/director"},
                                "o": {"value": "http://dbpedia.org/resource/Lana_Wachowski"}
                            },
                            {
                                "p": {"value": "http://dbpedia.org/ontology/title"},
                                "o": {"value": "The Matrix"}
                            }
                        ]
                    }
                }
            
            return mock_response
        
        mock_get.side_effect = mock_response_side_effect
        
        fetcher = RecursiveFetcher(config_path=temp_config_file)
        result = fetcher.fetch_recursively("http://dbpedia.org/resource/The_Matrix", max_depth=1)
        
        # Check that data was fetched
        assert "http://dbpedia.org/resource/The_Matrix" in result
        assert len(result) > 0
        
        # Check summary
        summary = fetcher.get_summary()
        assert summary['total_uris_fetched'] > 0
        assert "http://dbpedia.org/resource/The_Matrix" in summary['visited_uris']
    
    def test_fetcher_with_preloaded_config(self, sample_config):
        """Test fetcher with pre-loaded configuration."""
        config = OntologyConfigModel(**sample_config)
        fetcher = RecursiveFetcher(config=config)
        
        assert fetcher.config.name == "test"
        assert len(fetcher.config.ontology) == 2
    
    def test_fetcher_default_initialization(self):
        """Test fetcher with default initialization (uses movie config)."""
        fetcher = RecursiveFetcher()
        
        assert fetcher.config is not None
        assert fetcher.config.name == "movie"

if __name__ == "__main__":
    pytest.main([__file__]) 