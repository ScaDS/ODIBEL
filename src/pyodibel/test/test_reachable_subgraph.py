import pytest

from pyodibel.operations.rdf.nt_io import parse_nt_line
from pyodibel.operations.rdf.rdf2 import rDF2
from pyodibel.operations.rdf.reachability import (
    NTripleGraph,
    filter_reachable_from_classes,
    filter_reachable_from_entities,
    induce_subgraph_triples,
    reachable_entities,
)
from pyodibel.operations.rdf.uris import normalize_class_uri


class TestNormalizeClassUri:
    def test_dbo_prefix(self):
        assert rDF2.normalize_class_uri("dbo:Food") == "<http://dbpedia.org/ontology/Food>"

    def test_full_uri(self):
        value = "<http://dbpedia.org/ontology/Food>"
        assert rDF2.normalize_class_uri(value) == value


RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
FOOD = "<http://example.org/Food>"
CITY = "<http://example.org/City>"
COUNTRY = "<http://example.org/Country>"
REGION = "<http://example.org/region>"
IS_PART_OF = "<http://example.org/isPartOf>"


def _food_chain_rows():
    return [
        ("<food1>", RDF_TYPE, FOOD, False),
        ("<city1>", RDF_TYPE, CITY, False),
        ("<country1>", RDF_TYPE, COUNTRY, False),
        ("<city2>", RDF_TYPE, CITY, False),
        ("<food1>", REGION, "<city1>", False),
        ("<city1>", IS_PART_OF, "<country1>", False),
        ("<city2>", IS_PART_OF, "<country1>", False),
    ]


class TestMemoryReachability:
    def test_expands_along_object_edges(self):
        result = filter_reachable_from_classes(_food_chain_rows(), FOOD)
        entities = {subject for subject, _, _, _ in result}
        assert entities == {"<food1>", "<city1>", "<country1>"}
        assert "<city2>" not in entities

    def test_max_hops_limits_expansion(self):
        result = filter_reachable_from_classes(_food_chain_rows(), FOOD, max_hops=1)
        entities = {subject for subject, _, _, _ in result}
        assert entities == {"<food1>", "<city1>"}

    def test_induce_keeps_literals_and_types(self):
        rows = _food_chain_rows() + [
            ("<food1>", "<http://example.org/label>", '"Pizza"', True),
        ]
        graph = NTripleGraph.from_triples(rows)
        entities = reachable_entities(graph, FOOD)
        induced = induce_subgraph_triples(graph, entities)
        predicates = {predicate for _, predicate, _, _ in induced}
        assert RDF_TYPE in predicates
        assert "<http://example.org/label>" in predicates


class TestReachableFromSeeds:
    def _rows(self):
        return [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<food2>", RDF_TYPE, FOOD, False),
            ("<food3>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<country1>", RDF_TYPE, COUNTRY, False),
            ("<city2>", RDF_TYPE, CITY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<city1>", IS_PART_OF, "<country1>", False),
            ("<city2>", IS_PART_OF, "<country1>", False),
        ]

    def _rows_with_linked_food(self):
        return self._rows() + [
            ("<food1>", "<http://example.org/relatedFood>", "<food3>", False),
        ]

    def test_expands_only_from_given_seeds(self):
        graph = NTripleGraph.from_triples(self._rows())
        result = filter_reachable_from_entities(self._rows(), ["<food1>", "<food2>"])
        entities = {subject for subject, _, _, _ in result}
        assert entities == {"<food1>", "<food2>", "<city1>", "<country1>"}
        assert "<food3>" not in entities
        assert "<city2>" not in entities

    def test_main_class_scope_seeds_excludes_reachable_same_class(self):
        rows = self._rows_with_linked_food()
        result = filter_reachable_from_entities(
            rows,
            ["<food1>", "<food2>"],
            main_class=FOOD,
            main_class_scope="seeds",
        )
        entities = {subject for subject, _, _, _ in result}
        assert entities == {"<food1>", "<food2>", "<city1>", "<country1>"}
        assert "<food3>" not in entities

    def test_main_class_scope_reachable_includes_reachable_same_class(self):
        rows = self._rows_with_linked_food()
        result = filter_reachable_from_entities(
            rows,
            ["<food1>"],
            main_class=FOOD,
            main_class_scope="reachable",
        )
        entities = {subject for subject, _, _, _ in result}
        assert "<food3>" in entities

    def test_main_class_scope_seeds_requires_main_class(self):
        with pytest.raises(ValueError, match="main_class is required"):
            filter_reachable_from_entities(
                self._rows(),
                ["<food1>"],
                main_class_scope="seeds",
            )

    def test_filter_keeps_literals_and_types(self):
        rows = self._rows() + [
            ("<food1>", "<http://example.org/label>", '"Hero"', True),
        ]
        result = filter_reachable_from_entities(rows, ["<food1>"])
        predicates = {predicate for _, predicate, _, _ in result}
        assert RDF_TYPE in predicates
        assert "<http://example.org/label>" in predicates


class TestNtIo:
    def test_parse_nt_line(self):
        triple = parse_nt_line('<s> <p> "o"@en .')
        assert triple == ("<s>", "<p>", '"o"@en', True)


@pytest.fixture(scope="session")
def spark():
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("test_reachable_subgraph")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(f"Spark unavailable: {exc}")

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_rdf2(spark, rows):
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", StringType(), True),
        StructField("o", StringType(), True),
        StructField("isLiteral", BooleanType(), True),
    ])
    return rDF2(spark.createDataFrame(rows, schema=schema))


class TestFilterReachableFromClasses:
    def test_expands_along_object_edges(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<country1>", RDF_TYPE, COUNTRY, False),
            ("<city2>", RDF_TYPE, CITY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<city1>", IS_PART_OF, "<country1>", False),
            ("<city2>", IS_PART_OF, "<country1>", False),
        ]
        result = make_rdf2(spark, rows).filter_reachable_from_classes(FOOD)
        entities = {row.s for row in result.df.select("s").distinct().collect()}
        assert entities == {"<food1>", "<city1>", "<country1>"}
        assert "<city2>" not in entities

    def test_keeps_literals_and_types_for_reachable_entities(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<food1>", "<http://example.org/label>", '"Pizza"', True),
        ]
        result = make_rdf2(spark, rows).filter_reachable_from_classes(FOOD)
        predicates = {row.p for row in result.df.collect()}
        assert RDF_TYPE in predicates
        assert "<http://example.org/label>" in predicates

    def test_max_hops_limits_expansion(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<country1>", RDF_TYPE, COUNTRY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<city1>", IS_PART_OF, "<country1>", False),
        ]
        result = make_rdf2(spark, rows).filter_reachable_from_classes(FOOD, max_hops=1)
        entities = {row.s for row in result.df.select("s").distinct().collect()}
        assert entities == {"<food1>", "<city1>"}


class TestFilterReachableFromEntities:
    def test_expands_only_from_given_seeds(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<food2>", RDF_TYPE, FOOD, False),
            ("<food3>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<country1>", RDF_TYPE, COUNTRY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<city1>", IS_PART_OF, "<country1>", False),
        ]
        result = make_rdf2(spark, rows).filter_reachable_from_entities(["<food1>", "<food2>"])
        entities = {row.s for row in result.df.select("s").distinct().collect()}
        assert entities == {"<food1>", "<food2>", "<city1>", "<country1>"}
        assert "<food3>" not in entities

    def test_main_class_scope_seeds_excludes_reachable_same_class(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<food2>", RDF_TYPE, FOOD, False),
            ("<food3>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<country1>", RDF_TYPE, COUNTRY, False),
            ("<food1>", REGION, "<city1>", False),
            ("<city1>", IS_PART_OF, "<country1>", False),
            ("<food1>", "<http://example.org/relatedFood>", "<food3>", False),
        ]
        result = make_rdf2(spark, rows).filter_reachable_from_entities(
            ["<food1>", "<food2>"],
            main_class=FOOD,
            main_class_scope="seeds",
        )
        entities = {row.s for row in result.df.select("s").distinct().collect()}
        assert entities == {"<food1>", "<food2>", "<city1>", "<country1>"}
        assert "<food3>" not in entities


class TestFilterSubgraphByEntityClasses:
    def test_does_not_expand_to_other_classes(self, spark):
        rows = [
            ("<food1>", RDF_TYPE, FOOD, False),
            ("<city1>", RDF_TYPE, CITY, False),
            ("<food1>", REGION, "<city1>", False),
        ]
        result = make_rdf2(spark, rows).filter_subgraph_by_entity_classes([FOOD])
        entities = {row.s for row in result.df.select("s").distinct().collect()}
        assert entities == {"<food1>"}
