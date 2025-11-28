import csv
from pydantic import BaseModel
from joblib import Memory
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from rdflib import Graph
import requests


class LinkedDataResponse(BaseModel):
    content: bytes
    format: str


class LinkedDataClient():

    def get_rdf(self, uri: str) -> Graph:
        pass

def linked_data_http_request(uri: str) -> bytes:

    # weighted by popularity
    accept_headers = { 
        "application/n-triples": 0.5,
        "application/rdf+xml": 0.3,
        "application/json": 0.1,
        "application/turtle": 0.05,
        "application/ld+json": 0.05,
        "text/turtle": 0.05,
    }
    accept_header = max(accept_headers, key=accept_headers.get)
    response = requests.get(uri, headers={"Accept": accept_header})
    if response.status_code != 200:
        raise Exception(f"Request failed: {response.status_code} - {uri}")
    return LinkedDataResponse(content=response.content, format=response.headers.get("Content-Type", ""))

def rdf_parser(response: LinkedDataResponse) -> Graph:
    if response.format == "application/n-triples":
        return Graph().parse(data=response.content, format="nt")
    else:
        # try to parse <script></script> content
        script_content = response.content.decode("utf-8").split("<script>")[1].split("</script>")[0]
        return Graph().parse(data=script_content, format="json-ld")

class ExternalIDProperty(BaseModel):
    label: str
    uri: str
    format_string: str

    def format(self, external_id):
        return self.format_string.replace("$1", external_id)


FILE_PATH = "resources/wikidata_externalID_properties"

location = "./cachedir"
memory = Memory(location, verbose=0)

@memory.cache
def query_external_id_property(uri):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery(f"""
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT * WHERE {{
      VALUES ?property {{ <{uri}> }}
      ?wikidata_id ?p ?external_id .
      ?property wikibase:directClaim ?p .
    }} LIMIT 10
    """)
    sparql.setReturnFormat(JSON)
    for result in sparql.query().convert()["results"]["bindings"]:
        yield result["wikidata_id"]["value"], result["external_id"]["value"]

def main():
    
    external_id_properties = []

    with open(FILE_PATH, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            external_id_property = ExternalIDProperty(label=row[1], uri=row[0], format_string=row[2])
            external_id_properties.append(external_id_property)

    # for external_id_property in tqdm(external_id_properties, desc="Querying external ID properties"):
    #     results = query_external_id_property(external_id_property.uri)
    #     print(external_id_property.label)
    #     for wikidata_id, external_id in results:
    #         print(external_id_property.format(external_id))



    content = linked_data_http_request("https://ifsc.bankifsccode.com/UTIB0000022")
    print(content)




if __name__ == "__main__":
    main()