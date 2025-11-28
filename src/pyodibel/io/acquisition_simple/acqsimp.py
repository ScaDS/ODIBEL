import requests
import os
from rdflib import Graph
from urllib.parse import urlparse
import re

WIKIDATA_ENTITY_NAMESPACE = "http://www.wikidata.org/entity/"


def extract_filename_from_uri(uri):
    # If it's a Wikidata entity URI, use the entity ID
    if uri.startswith(WIKIDATA_ENTITY_NAMESPACE):
        return uri.split("/")[-1]
    # Otherwise, use the last path segment
    parsed_uri = urlparse(uri)
    filename = os.path.basename(parsed_uri.path)
    if not filename:
        filename = "entity"
    return filename


# def convert_to_rdf_endpoint(uri):
#     """
#     Convert a Wikidata entity page URL to its RDF endpoint.
#     """
#     # Check if it's a Wikidata entity page
#     wikidata_match = re.match(r'https://www\.wikidata\.org/wiki/(Q\d+)', uri)
#     if wikidata_match:
#         entity_id = wikidata_match.group(1)
#         return f"https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.rdf"
    
#     # For other URIs, try to append .rdf if it doesn't already have an extension
#     if not uri.endswith(('.rdf', '.xml', '.ttl', '.nt')):
#         return f"{uri}.rdf"
    
#     return uri


def fetch_and_store_rdf(uri, output_dir="./data"):
    """
    Fetch a URI as RDF and store it in the specified directory.
    The filename will be the last path segment of the URI or the entity ID for Wikidata entities.
    
    Args:
        uri (str): The URI to fetch
        output_dir (str): Directory to store the RDF file
    
    Returns:
        str: Path to the saved file
    """
    # Convert to RDF endpoint if needed
    # rdf_uri = convert_to_rdf_endpoint(uri)
    
    # Set headers to request RDF/XML
    headers = {
        "Accept": "application/n-triples"
    }
    
    try:
        # Fetch the URI
        response = requests.get(uri, headers=headers)
        response.raise_for_status()
        
        print(response.text)
        # Parse the RDF data
        graph = Graph()
        graph.parse(data=response.text, format="nt")
        
        # Use the correct filename extraction
        filename = extract_filename_from_uri(uri)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the RDF graph
        output_path = os.path.join(output_dir, f"{filename}.nt")
        graph.serialize(destination=output_path, format="ntriples")
        
        print(f"✅ Successfully fetched and stored: {uri} -> {output_path}")
        return output_path
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch {uri}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error processing {uri}: {e}")
        return None


def fetch_multiple_uris(uris, output_dir="./data"):
    """
    Fetch multiple URIs and store them as RDF files.
    
    Args:
        uris (list): List of URIs to fetch
        output_dir (str): Directory to store the RDF files
    
    Returns:
        list: List of successfully saved file paths
    """
    successful_fetches = []
    
    for uri in uris:
        result = fetch_and_store_rdf(uri, output_dir)
        if result:
            successful_fetches.append(result)
    
    print(f"\n📊 Summary: {len(successful_fetches)}/{len(uris)} URIs successfully fetched")
    return successful_fetches


# Keep the original function for backward compatibility
def get_wikidata_entity(entity_uri, category, allowed_properties):
    """
    Legacy function for backward compatibility.
    Now uses the simplified fetch_and_store_rdf function.
    """
    result = fetch_and_store_rdf(entity_uri, f"./data/{category}")
    if result:
        # Return a graph for compatibility
        graph = Graph()
        graph.parse(result, format="nt")
        return graph
    return None