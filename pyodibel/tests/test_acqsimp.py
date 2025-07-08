from pyodibel.acquisition_simple.acqsimp import get_wikidata_entity, fetch_and_store_rdf
from rdflib import Graph
import os


def test_get_wikidata_entity():
    # Use a real Wikidata entity (The Matrix movie)
    entity_uri = "http://www.wikidata.org/entity/Q604"
    category = "film"
    # Use actual Wikidata property URIs
    allowed_properties = [
        "http://www.wikidata.org/prop/direct/P136",  # genre
        "http://www.wikidata.org/prop/direct/P57",   # director
        "http://www.wikidata.org/prop/direct/P161"   # cast member
    ]
    entity = get_wikidata_entity(entity_uri, category, allowed_properties)
    assert entity is not None
    assert len(entity) > 0
    assert entity.serialize(format="turtle") is not None


def test_fetch_and_store_rdf():
    # Use a real Wikidata entity (The Matrix movie)
    entity_uri = "http://www.wikidata.org/entity/Q604"
    output_dir = "./test_output"
    expected_filename = os.path.join(output_dir, "Q604.rdf")

    # Remove the file if it exists from a previous test
    if os.path.exists(expected_filename):
        os.remove(expected_filename)

    # Fetch and store RDF
    result_path = fetch_and_store_rdf(entity_uri, output_dir)
    assert result_path is not None
    assert result_path == expected_filename
    assert os.path.exists(result_path)

    # Check that the file contains valid RDF/XML
    g = Graph()
    g.parse(result_path, format="xml")
    assert len(g) > 0