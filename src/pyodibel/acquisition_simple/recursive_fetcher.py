import requests
import json
from typing import Set, Dict, List, Optional, Tuple, Union
from urllib.parse import quote
from .config import load_config, OntologyConfigModel, PropertyModel
import os
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS

class RecursiveFetcher:
    """
    Recursively fetches RDF data starting from a URI, following only properties
    defined in the ontology configuration.
    """
    
    def __init__(self, config_path: Optional[str] = None, config: Optional[OntologyConfigModel] = None):
        """
        Initialize the fetcher with a configuration.
        
        Args:
            config_path: Path to the configuration file
            config: Pre-loaded configuration object
        """
        if config:
            self.config = config
        elif config_path:
            self.config = load_config(config_path)
        else:
            # Default to movie config for backward compatibility
            from .config import get_movie_ontology
            self.config = get_movie_ontology()
            
        self.visited_uris: Set[str] = set()
        self.fetched_data: Dict[str, Union[Dict, Graph]] = {}
        
    def get_configured_properties(self) -> Set[str]:
        """Get all properties defined in the configuration."""
        properties = set()
        for ontology_class in self.config.ontology:
            for prop in ontology_class.properties:
                properties.add(prop.property)
        return properties
    
    def get_configured_classes(self) -> Set[str]:
        """Get all classes defined in the configuration."""
        classes = set()
        for ontology_class in self.config.ontology:
            classes.add(ontology_class.class_)
        return classes
    
    def get_property_config(self, class_name: str, property_uri: str) -> Optional[PropertyModel]:
        """Get the property configuration for a given class and property."""
        for ontology_class in self.config.ontology:
            if ontology_class.class_ == class_name:
                for prop in ontology_class.properties:
                    if prop.property == property_uri:
                        return prop
                break
        return None
    
    def get_followable_properties(self, class_name: str) -> List[str]:
        """Get properties that should be followed for a given class."""
        followable_props = []
        for ontology_class in self.config.ontology:
            if ontology_class.class_ == class_name:
                for prop in ontology_class.properties:
                    if prop.range:  # Only follow properties with a range (object properties)
                        followable_props.append(prop.property)
                break
        return followable_props
    
    def get_class_for_uri(self, uri: str) -> Optional[str]:
        """Determine the class of a URI using direct RDF fetching."""
        try:
            # Fetch RDF data directly from the URI
            response = requests.get(
                uri,
                headers={'Accept': 'application/rdf+xml'}
            )
            
            if response.status_code == 200:
                g = Graph()
                g.parse(data=response.text, format='xml')
                
                # Get all configured classes for comparison
                configured_classes = self.get_configured_classes()
                
                # Find rdf:type triples and return the first configured class
                for s, p, o in g.triples((URIRef(uri), RDF.type, None)):
                    if str(o) in configured_classes:
                        return str(o)
        except Exception as e:
            print(f"Error getting class for {uri}: {e}")
        
        return None
    
    def fetch_uri_as_graph(self, uri: str) -> Graph:
        """Fetch URI data as an rdflib Graph using direct RDF fetching."""
        try:
            # Fetch RDF data directly from the URI
            response = requests.get(
                uri,
                headers={'Accept': 'application/rdf+xml'}
            )
            
            if response.status_code == 200:
                g = Graph()
                g.parse(data=response.text, format='xml')
                return g
        except Exception as e:
            print(f"Error fetching graph for {uri}: {e}")
        
        return Graph()
    
    def filter_graph_by_config(self, graph: Graph, uri: str) -> Graph:
        """
        Filter graph to only include properties defined in the configuration
        for the specific class of the URI, and rdf:type triples only for configured classes.
        Also processes 'as' field to fetch literal values instead of URIs.
        """
        configured_classes = self.get_configured_classes()
        
        filtered_graph = Graph()
        subject = URIRef(uri)
        
        # First, determine the class of this URI
        class_name = self.get_class_for_uri(uri)
        
        # Get properties configured for this specific class
        class_properties = set()
        property_configs = {}  # Store property configs for 'as' processing
        if class_name:
            for ontology_class in self.config.ontology:
                if ontology_class.class_ == class_name:
                    for prop in ontology_class.properties:
                        class_properties.add(prop.property)
                        property_configs[prop.property] = prop
                    break
        
        for s, p, o in graph.triples((subject, None, None)):
            # Handle rdf:type specially - only include configured classes
            if p == RDF.type:
                # Check if this type is in our configured classes
                if str(o) in configured_classes or any(str(o).endswith(f"/{cls.split(':')[-1]}") for cls in configured_classes):
                    filtered_graph.add((s, p, o))
            
            # For other properties, only include if they're configured for this specific class
            elif class_name and str(p) in class_properties:
                prop_config = property_configs.get(str(p))
                
                # Check if this property has an 'as' field that should fetch literal values
                if prop_config and prop_config.should_fetch_specific_property() and isinstance(o, URIRef):
                    # Fetch the specific property value (like rdfs:label) instead of using the URI
                    fetch_property = prop_config.get_fetch_property()
                    fetch_language = prop_config.get_fetch_language()
                    if fetch_property:
                        literal_value = self.fetch_specific_property(str(o), fetch_property, fetch_language)
                        if literal_value:
                            # Add the literal value instead of the URI
                            filtered_graph.add((s, p, Literal(literal_value)))
                        else:
                            # Fallback to URI if property not found
                            filtered_graph.add((s, p, o))
                    else:
                        # Fallback to URI if no fetch property specified
                        filtered_graph.add((s, p, o))
                else:
                    # No special processing needed, add as is
                    filtered_graph.add((s, p, o))
        
        return filtered_graph
    
    def graph_to_dict(self, graph: Graph, uri: str) -> Dict[str, List[str]]:
        """Convert rdflib Graph to dictionary format."""
        data = {}
        subject = URIRef(uri)
        
        for s, p, o in graph.triples((subject, None, None)):
            pred_str = str(p)
            obj_str = str(o)
            
            if pred_str not in data:
                data[pred_str] = []
            data[pred_str].append(obj_str)
        
        return data
    
    def fetch_specific_property(self, uri: str, property_uri: str, language: Optional[str] = None) -> Optional[str]:
        """Fetch a specific property value for a URI using direct RDF fetching."""
        try:
            # Fetch RDF data directly from the URI
            response = requests.get(
                uri,
                headers={'Accept': 'application/rdf+xml'}
            )
            
            if response.status_code == 200:
                g = Graph()
                g.parse(data=response.text, format='xml')
                
                # Find the specific property value
                for s, p, o in g.triples((URIRef(uri), URIRef(property_uri), None)):
                    if language:
                        # If language is specified, check if the literal has the right language
                        if isinstance(o, Literal) and hasattr(o, 'language') and o.language == language:
                            return str(o)
                    else:
                        # No language specified, return the first value found
                        return str(o)
        except Exception as e:
            print(f"Error fetching property {property_uri} for {uri}: {e}")
        
        return None
    
    def process_property_values(self, uri: str, class_name: str, property_uri: str, objects: List[str]) -> List[str]:
        """
        Process property values based on configuration.
        If the property has an 'as' field, fetch the specific property instead of using URIs.
        """
        prop_config = self.get_property_config(class_name, property_uri)
        if not prop_config:
            return objects
        
        if prop_config.should_fetch_specific_property():
            # Fetch specific property (like rdfs:label) instead of using URIs
            fetch_property = prop_config.get_fetch_property()
            fetch_language = prop_config.get_fetch_language()
            processed_objects = []
            
            for obj in objects:
                if obj.startswith('http://dbpedia.org/resource/'):
                    # Fetch the specific property value
                    if fetch_property:
                        property_value = self.fetch_specific_property(obj, fetch_property, fetch_language)
                        if property_value:
                            processed_objects.append(property_value)
                        else:
                            # Fallback to URI if property not found
                            processed_objects.append(obj)
                    else:
                        # Fallback to URI if no fetch property specified
                        processed_objects.append(obj)
                else:
                    # Keep literal values as is
                    processed_objects.append(obj)
            
            return processed_objects
        else:
            # No special processing needed
            return objects
    
    def is_followable_uri(self, uri: str, property_uri: str, class_name: str) -> bool:
        """Check if a URI should be followed based on the property and class."""
        # Check if the property is in the followable properties for this class
        followable_props = self.get_followable_properties(class_name)
        
        # Check if the URI is from DBpedia (to avoid following external URIs)
        if not uri.startswith('http://dbpedia.org/resource/'):
            return False
            
        return property_uri in followable_props
    
    def fetch_recursively(self, start_uri: str, max_depth: int = 3, current_depth: int = 0, return_graph: bool = False) -> Union[Dict, Graph]:
        """
        Recursively fetch data starting from a URI.
        
        Args:
            start_uri: The starting URI to fetch
            max_depth: Maximum recursion depth
            current_depth: Current recursion depth
            return_graph: If True, return the filtered RDF graph instead of dictionary
            
        Returns:
            Dictionary containing the fetched data for the starting URI, or Graph if return_graph=True
        """
        if current_depth >= max_depth or start_uri in self.visited_uris:
            return {} if not return_graph else Graph()
        
        print(f"Fetching {start_uri} (depth {current_depth})")
        self.visited_uris.add(start_uri)
        
        # Fetch data as rdflib Graph
        graph = self.fetch_uri_as_graph(start_uri)
        
        # Filter graph to only include configured properties and classes
        filtered_graph = self.filter_graph_by_config(graph, start_uri)
        
        # Store data for the main URI (depth 0)
        if current_depth == 0:
            if return_graph:
                self.fetched_data[start_uri] = filtered_graph
            else:
                # Convert to dictionary format
                filtered_data = self.graph_to_dict(filtered_graph, start_uri)
                
                # Determine the class of this URI
                class_name = self.get_class_for_uri(start_uri)
                
                # Process property values based on configuration
                processed_data = {}
                for prop_uri, objects in filtered_data.items():
                    if class_name:
                        processed_objects = self.process_property_values(start_uri, class_name, prop_uri, objects)
                        processed_data[prop_uri] = processed_objects
                    else:
                        processed_data[prop_uri] = objects
                
                self.fetched_data[start_uri] = processed_data
        
        if not return_graph:
            # Determine the class of this URI
            class_name = self.get_class_for_uri(start_uri)
            
            if not class_name:
                return processed_data if current_depth == 0 else {}
            
            # Get followable properties for this class
            followable_props = self.get_followable_properties(class_name)
            
            # Follow URIs that are objects of followable properties
            for s, p, o in graph.triples((URIRef(start_uri), None, None)):
                if str(p) in followable_props and isinstance(o, URIRef):
                    obj_uri = str(o)
                    if obj_uri.startswith('http://dbpedia.org/resource/'):
                        if self.is_followable_uri(obj_uri, str(p), class_name):
                            # Recursively fetch the object URI (but don't store its data)
                            self.fetch_recursively(obj_uri, max_depth, current_depth + 1, return_graph=False)
            
            # Return only the data for the starting URI
            return processed_data if current_depth == 0 else {}
        else:
            # For graph mode, just return the filtered graph
            return filtered_graph if current_depth == 0 else Graph()
    
    def get_summary(self) -> Dict:
        """Get a summary of fetched data."""
        return {
            'total_uris_fetched': len(self.fetched_data),
            'visited_uris': list(self.visited_uris),
            'data': self.fetched_data
        }

def main():
    """Example usage with different configurations."""
    
    # Example 1: Using movie config (backward compatibility)
    print("=== Example 1: Movie ontology ===")
    fetcher1 = RecursiveFetcher()  # Uses default movie config
    result1 = fetcher1.fetch_recursively("http://dbpedia.org/resource/The_Matrix", max_depth=1)
    summary1 = fetcher1.get_summary()
    print(f"Fetched {summary1['total_uris_fetched']} URIs")
    
    # Example 2: Using custom config file
    print("\n=== Example 2: Custom ontology ===")
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'example.conf')
    fetcher2 = RecursiveFetcher(config_path=config_path)
    result2 = fetcher2.fetch_recursively("http://dbpedia.org/resource/Keanu_Reeves", max_depth=1)
    summary2 = fetcher2.get_summary()
    print(f"Fetched {summary2['total_uris_fetched']} URIs")
    
    # Example 3: Using pre-loaded config
    print("\n=== Example 3: Pre-loaded config ===")
    config = load_config(config_path)
    fetcher3 = RecursiveFetcher(config=config)
    result3 = fetcher3.fetch_recursively("http://dbpedia.org/resource/Los_Angeles", max_depth=1)
    summary3 = fetcher3.get_summary()
    print(f"Fetched {summary3['total_uris_fetched']} URIs")
    
    return summary1, summary2, summary3

if __name__ == "__main__":
    main() 