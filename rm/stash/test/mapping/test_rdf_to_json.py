from pyodibel.rdf_ops.utils import build_recursive_json
from rdflib import Graph
from pyodibel.rdf_ops.construct import DirectMappingType
import json

def test_rdf_to_json():
    turtle_data = """
    @prefix ex: <http://dbpedia.org/resource/> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

    ex:The_Matrix a ex:Movie;
        rdfs:label "The Matrix"@en;
        ex:starring ex:Keanu_Reeves;
        ex:starring ex:Laurence_Fishburne;
        ex:director ex:The_Wachowski_Brothers;
        ex:writer ex:The_Wachowski_Brothers;
        ex:producer ex:The_Wachowski_Brothers;
        ex:genre ex:Science_Fiction;
        ex:runtime 136;
        ex:budget 63000000;
        ex:gross 463737373;
        ex:releaseDate "1999-03-31";
        ex:releaseCountry "United States" .

    ex:Laurence_Fishburne a ex:Person;
        rdfs:label "Laurence Fishburne"@en;
        ex:birthDate "1961-07-30";
        ex:nationality ex:American .

    ex:Keanu_Reeves a ex:Person;
        rdfs:label "Keanu Reeves"@en;
        ex:birthDate "1964-09-02";
        ex:birthPlace ex:Toronto;
        ex:nationality ex:Canadian .

    ex:The_Wachowski_Brothers a ex:Person;
        rdfs:label "The Wachowski Brothers"@en .

    ex:Science_Fiction a ex:Genre;
        rdfs:label "Science Fiction"@en .
    """

    prefix_map = {
        "ex": "http://dbpedia.org/resource/",
    }

    mappings = {
        f"{prefix_map['ex']}name": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}starring": DirectMappingType.OBJECT,
        f"{prefix_map['ex']}director": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}writer": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}producer": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}genre": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}runtime": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}budget": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}gross": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}releaseDate": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}releaseCountry": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}birthDate": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}birthPlace": DirectMappingType.LITERAL,
        f"{prefix_map['ex']}nationality": DirectMappingType.LITERAL,
    }

    graph = Graph()
    graph.parse(data=turtle_data, format="turtle")
    
    json_data, prov = build_recursive_json("http://dbpedia.org/resource/The_Matrix", graph, mappings=mappings, trace=True)
    print("JSON Data:")
    print(json.dumps(json_data, indent=4))
    print("\nProvenance:")
    print(json.dumps(prov, indent=4))