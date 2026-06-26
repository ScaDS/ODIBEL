import pytest

from pyodibel.operations.sample.core import (
    allocate_quotas,
    relation_distribution,
    sample_stratified_groups,
    validate_sample_size,
)
from pyodibel.operations.sample.stratified import stratified_sample_triples
from pyodibel.operations.sample.subgraph import degree_bin_label


class TestCore:
    def test_allocate_quotas_sums_to_target(self):
        quotas = allocate_quotas({"a": 100, "b": 50, "c": 10}, 32)
        assert sum(quotas.values()) == 32
        assert quotas == {"a": 20, "b": 10, "c": 2}

    def test_validate_sample_size_fraction(self):
        assert validate_sample_size(fraction=0.2, n=None, total=160) == 32

    def test_validate_sample_size_requires_exactly_one(self):
        with pytest.raises(ValueError, match="exactly one"):
            validate_sample_size(fraction=0.2, n=10, total=100)


class TestStratifiedTriples:
    def test_preserves_relation_distribution(self):
        triples = [(f"s{i}", "<relA>", f"o{i}") for i in range(100)]
        triples += [(f"s{i}", "<relB>", f"o{i}") for i in range(50)]
        triples += [(f"s{i}", "<relC>", f"o{i}") for i in range(10)]

        sampled = stratified_sample_triples(triples, fraction=0.2, seed=42)
        assert len(sampled) == 32

        def rel_frac(ts):
            dist = relation_distribution(ts)
            total = len(ts)
            return {k: v / total for k, v in dist.items()}

        orig = rel_frac(triples)
        samp = rel_frac(sampled)
        for key in orig:
            assert orig[key] == pytest.approx(samp[key])

    def test_sample_stratified_groups_exact_n(self):
        groups = {"x": list(range(10)), "y": list(range(10, 30))}
        sampled = sample_stratified_groups(groups, 8, seed=1)
        assert len(sampled) == 8


class TestDegreeBins:
    def test_degree_bin_label(self):
        assert degree_bin_label(1) == "1-5"
        assert degree_bin_label(5) == "1-5"
        assert degree_bin_label(6) == "6-20"
        assert degree_bin_label(101) == "101+"


@pytest.fixture(scope="session")
def spark():
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("test_sample")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(f"Spark unavailable: {exc}")

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_rdf2(spark, rows):
    from pyodibel.operations.rdf.rdf2 import rDF2
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    schema = StructType([
        StructField("s", StringType(), True),
        StructField("p", StringType(), True),
        StructField("o", StringType(), True),
        StructField("isLiteral", BooleanType(), True),
    ])
    return rDF2(spark.createDataFrame(rows, schema=schema))


RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
PERSON = "<http://example.org/Person>"
FOOD = "<http://example.org/Food>"
COUNTRY = "<http://example.org/country>"


class TestRepresentativeSubgraph:
    def test_keeps_all_triples_for_selected_entities(self, spark):
        rows = [
            ("<e1>", RDF_TYPE, PERSON, False),
            ("<e2>", RDF_TYPE, PERSON, False),
            ("<e3>", RDF_TYPE, FOOD, False),
            ("<e1>", COUNTRY, '"DE"', True),
            ("<e1>", "<http://example.org/name>", '"Alice"', True),
            ("<e2>", COUNTRY, '"US"', True),
            ("<e3>", "<http://example.org/name>", '"Pizza"', True),
        ]
        from pyodibel.operations.sample.subgraph import sample_representative_subgraph

        rdf = make_rdf2(spark, rows)
        sampled = sample_representative_subgraph(rdf, n=2, seed=1)
        entities = {row.s for row in sampled.df.select("s").distinct().collect()}
        assert len(entities) == 2
        for entity in entities:
            original_preds = {
                row.p for row in rdf.df.filter(f"s = '{entity}'").select("p").collect()
            }
            sampled_preds = {
                row.p for row in sampled.df.filter(f"s = '{entity}'").select("p").collect()
            }
            assert sampled_preds == original_preds

    def test_entity_count_matches_target(self, spark):
        rows = [(f"<e{i}>", RDF_TYPE, PERSON, False) for i in range(20)]
        rows += [(f"<e{i}>", COUNTRY, f'"C{i}"', True) for i in range(20)]
        from pyodibel.operations.sample.subgraph import sample_representative_subgraph

        sampled = sample_representative_subgraph(make_rdf2(spark, rows), fraction=0.25, seed=3)
        assert sampled.df.select("s").distinct().count() == 5
