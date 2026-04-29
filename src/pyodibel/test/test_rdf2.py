import pytest
from unittest.mock import patch
from io import BytesIO

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

MINI_ONTOLOGY = b"""
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <http://example.org/> .

ex:Animal    a rdfs:Class .
ex:Mammal    rdfs:subClassOf ex:Animal .
ex:Dog       rdfs:subClassOf ex:Mammal .

ex:hasPet rdfs:domain ex:Person ;
          rdfs:range  ex:Animal .

ex:name rdfs:domain ex:Animal ;
        rdfs:range  rdfs:Literal .
"""

RDF_TYPE_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
EX = "http://example.org/"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_clean_domain_range_violations")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_rdf2(spark: SparkSession, rows: list[tuple[str, str, str, bool]]):
    from pyodibel.operations.rdf.rdf2 import rDF2
    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", StringType(), True),
        StructField("o", StringType(), True),
        StructField("isLiteral", StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    return rDF2(df)


def patch_ontology():
    original_parse = __import__("rdflib").Graph.parse

    def fake_parse(self, source=None, *args, **kwargs):
        if isinstance(source, str) and source.startswith("http"):
            return original_parse(self, BytesIO(MINI_ONTOLOGY), format="turtle")
        return original_parse(self, source, *args, **kwargs)

    return patch("rdflib.Graph.parse", new=fake_parse)


class TestCleanDomainRangeViolations:

    def test_valid_object_property_kept(self, spark):
        """
        hasPet Domain ex:Person Range ex:Animal
        (Person, hasPet, Dog) is valid, because
        Dog is Mammal and Mammal is Animal
        """
        rows = [
            (f"<{EX}alice>", RDF_TYPE_URI, f"<{EX}Person>", False),
            (f"<{EX}rex>",   RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}alice>", f"<{EX}hasPet>", f"<{EX}rex>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}hasPet>").collect()
        assert len(output) == 1, "Valid Triple was deleted"

    def test_domain_violation_removed(self, spark):
        """
        hasPet Domain ex:Person.
        Triple (Dog, hasPet, Dog) Domain violation
        """
        rows = [
            (f"<{EX}rex>",  RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}buddy>", RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}rex>", f"<{EX}hasPet>", f"<{EX}buddy>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}hasPet>").collect()
        assert len(output) == 0, "Domain-violating Triple should be removed"

    def test_range_violation_wrong_class_removed(self, spark):
        """
        hasPet Range ex:Animal
        Triple (Person, hasPet, Person) violates Range
        """
        rows = [
            (f"<{EX}alice>", RDF_TYPE_URI, f"<{EX}Person>", False),
            (f"<{EX}bob>",   RDF_TYPE_URI, f"<{EX}Person>", False),
            (f"<{EX}alice>", f"<{EX}hasPet>", f"<{EX}bob>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}hasPet>").collect()
        assert len(output) == 0, "Range-violating Triple should be removed"

    def test_valid_literal_range_kept(self, spark):
        """
        ex:name Range rdfs:Literal.
        Triple (Dog, name, "Rex") is valid
        """
        rows = [
            (f"<{EX}rex>", RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}rex>", f"<{EX}name>", '"Rex"', True),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}name>").collect()
        assert len(output) == 1, "Valid Literal-Triple should be kept"

    def test_range_violation_iri_instead_of_literal_removed(self, spark):
        """
        ex:name rdfs:Literal Range.
        Triple with IRI as Objekt violates Range
        """
        rows = [
            (f"<{EX}rex>", RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}rex>", f"<{EX}name>", f"<{EX}SomeThing>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}name>").collect()
        assert len(output) == 0, "Range-violating Triple should be removed"

    def test_transitive_subclass_domain_valid(self, spark):
        """
        ex:name Domain ex:Animal
        existing hierarchy: Dog - Mammal - Animal
        ex:name property for Dog is valid
        """
        rows = [
            (f"<{EX}rex>", RDF_TYPE_URI, f"<{EX}Dog>", False),
            (f"<{EX}rex>", f"<{EX}name>", '"Rex"', True),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}name>").collect()
        assert len(output) == 1, "Dog is an Animal, Triple should be kept"

    def test_unknown_predicate_kept(self, spark):
        """
        Predicates without Domain/Range should be kept
        """
        rows = [
            (f"<{EX}alice>", RDF_TYPE_URI, f"<{EX}Person>", False),
            (f"<{EX}alice>", f"<{EX}unknownProp>", '"some value"', True),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}unknownProp>").collect()
        assert len(output) == 1, "Tripel with unknown predicate should be kept"

    def test_rdf_type_triples_kept(self, spark):
        """
        rdf:type has no Domain/Range in Mini-Ontology
        all type-Triple should be kept
        """
        rows = [
            (f"<{EX}alice>", RDF_TYPE_URI, f"<{EX}Person>", False),
            (f"<{EX}rex>",   RDF_TYPE_URI, f"<{EX}Dog>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == RDF_TYPE_URI).collect()
        assert len(output) == 2, "rdf:type-Triple should be kept"

    def test_empty_dataframe(self, spark):
        rows = []
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        assert result.df.count() == 0, "Empty Input should return empty dataframe"

    def test_return_type_is_rdf2(self, spark):
        from pyodibel.operations.rdf.rdf2 import rDF2

        rows = [
            (f"<{EX}alice>", RDF_TYPE_URI, f"<{EX}Person>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        assert isinstance(result, rDF2), "Returned object must be rDF2-Instance"

    def test_subject_without_type_and_domain_constraint_removed(self, spark):
        """
        Subject has no rdf:type
        Predicate has Domain
        Triple should be removed
        """
        rows = [
            ("<{EX}mystery>", f"<{EX}hasPet>", "<{EX}rex>", False),
        ]
        with patch_ontology():
            result = make_rdf2(spark, rows).clean_domain_range_violations()

        output = result.df.filter(F.col("p") == f"<{EX}hasPet>").collect()
        assert len(output) == 0, "Subject without rdf:type should be removed if predicate has domain"