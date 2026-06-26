from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from pyodibel.operations.rdf.replacer import (
    hashed_resource_uri,
    replace_namespace,
    replace_with_func_on_namespace,
    shade_reference_graph,
)


def _food_graph() -> Graph:
    graph = Graph()
    food = URIRef("http://dbpedia.org/ontology/Food")
    hero = URIRef("http://dbpedia.org/resource/Hero")
    city = URIRef("http://dbpedia.org/resource/Gotham")
    region = URIRef("http://dbpedia.org/ontology/region")

    graph.add((hero, RDF.type, food))
    graph.add((hero, region, city))
    return graph


class TestShadeReferenceGraph:
    def test_hashes_resources_and_maps_ontology(self):
        shaded = shade_reference_graph(_food_graph(), copy_labels=False)

        hero_shaded = URIRef(hashed_resource_uri("http://dbpedia.org/resource/Hero"))
        city_shaded = URIRef(hashed_resource_uri("http://dbpedia.org/resource/Gotham"))
        food_shaded = URIRef("http://kg.org/ontology/Food")
        region_shaded = URIRef("http://kg.org/ontology/region")

        assert (hero_shaded, RDF.type, food_shaded) in shaded
        assert (hero_shaded, region_shaded, city_shaded) in shaded
        assert not any(
            str(term).startswith("http://dbpedia.org/resource/")
            for term in shaded.all_nodes()
            if isinstance(term, URIRef)
        )

    def test_split_scoped_rdf_namespace(self):
        shaded = shade_reference_graph(
            _food_graph(),
            resource_namespace="http://kg.org/rdf/2/resource/",
            ontology_target_ns="http://kg.org/rdf/2/ontology/",
            copy_labels=False,
        )

        hero = URIRef(
            hashed_resource_uri(
                "http://dbpedia.org/resource/Hero",
                resource_namespace="http://kg.org/rdf/2/resource/",
            )
        )
        food = URIRef("http://kg.org/rdf/2/ontology/Food")
        assert (hero, RDF.type, food) in shaded


class TestReplacer:
    def test_replace_namespace(self):
        graph = Graph()
        graph.add(
            (
                URIRef("http://dbpedia.org/resource/A"),
                URIRef("http://dbpedia.org/ontology/name"),
                URIRef("http://dbpedia.org/resource/B"),
            )
        )
        replaced = replace_namespace(
            graph,
            "http://dbpedia.org/ontology/",
            "http://kg.org/ontology/",
        )
        assert (
            URIRef("http://dbpedia.org/resource/A"),
            URIRef("http://kg.org/ontology/name"),
            URIRef("http://dbpedia.org/resource/B"),
        ) in replaced

    def test_replace_with_func_on_namespace(self):
        graph = Graph()
        graph.add(
            (
                URIRef("http://dbpedia.org/resource/A"),
                URIRef("http://dbpedia.org/ontology/name"),
                URIRef("http://dbpedia.org/resource/B"),
            )
        )
        replaced = replace_with_func_on_namespace(
            graph,
            lambda uri: hashed_resource_uri(uri),
            "http://dbpedia.org/resource/",
        )
        assert (
            URIRef(hashed_resource_uri("http://dbpedia.org/resource/A")),
            URIRef("http://dbpedia.org/ontology/name"),
            URIRef(hashed_resource_uri("http://dbpedia.org/resource/B")),
        ) in replaced
