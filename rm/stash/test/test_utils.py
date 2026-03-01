from rdflib import Graph
from pyshacl import validate    

data="""
{
   "@context" : "https://databus.dbpedia.org/res/context.jsonld",
   "@graph" : {
      "@id" : "https://databus.dbpedia.org/gg46ixav/group-1/artifact-1/1.0.1",
      "@type" : [
         "Version",
         "Dataset"
      ],
      "abstract" : "A short abstract.",
      "artifact" : "https://databus.dbpedia.org/gg46ixav/group-1/artifact-1",
      "attribution" : "Author Name",
      "description" : "A longer description of the test dataset.",
      "distribution" : [
         {
            "@id" : "https://databus.dbpedia.org/gg46ixav/group-1/artifact-1/1.0.1#part1",
            "@type" : "Part",
            "byteSize" : 4629,
            "compression" : "none",
            "dcv:test" : "test1",
            "downloadURL" : "https://cloud.scadsai.uni-leipzig.de/remote.php/dav/files/p-9576-8848%40uni-leipzig.de/Kollektive/IT-Infrastructure-ULE/Security/Certificate-Signing/Readme.md",
            "file" : "https://cloud.scadsai.uni-leipzig.de/remote.php/dav/files/p-9576-8848%40uni-leipzig.de/Kollektive/IT-Infrastructure-ULE/Security/Certificate-Signing/Readme.md",
            "formatExtension" : "none",
            "hasVersion" : "1.0.1",
            "issued" : "2025-12-01T12:32:41Z",
            "sha256sum" : "ef17c24b89abe61beb172adc0eac686130c18cd5280d63f5a5d0666bf06812a2"
         }
      ],
      "group" : "https://databus.dbpedia.org/gg46ixav/group-1",
      "hasVersion" : "1.0.1",
      "issued" : "2025-12-01T12:32:41Z",
      "license" : "https://example.com/license",
      "modified" : "2025-12-01T12:32:41Z",
      "publisher" : "https://databus.dbpedia.org/gg46ixav",
      "title" : "Test Dataset",
      "wasDerivedFrom" : "https://example-source.com"
   }
}
"""

def test_shacl_validation():
    graph = Graph()
    graph.parse(data=data, format="json-ld")

    parsed_graph = Graph()
    parsed_graph.parse(data=graph.serialize(format="nt"), format="nt")

    shacl_graph = Graph()
    shacl_graph.parse("/home/marvin/phd/data/version.shacl", format="turtle")
    conforms, v_graph, v_text = validate(parsed_graph, shacl_graph=shacl_graph, inference="rdfs", abort_on_first=False, serialize_report_graph=True)