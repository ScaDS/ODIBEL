from rdflib import RDF, RDFS
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import Window
import os
from typing import Iterable
from pyodibel.management.spark_mgr import get_spark_session

NT_RE = r'^\s*(<[^>]*>|_:[A-Za-z0-9_]+|[^\s]+)\s+' \
        r'(<[^>]*>|[^\s]+)\s+' \
        r'((?:"(?:\\.|[^"\\])*"(?:@[A-Za-z-]+|\^\^<[^>]*>|\^\^[^\s]+)?)|(?:<[^>]*>|_:[A-Za-z0-9_]+|[^\s]+))\s*\.\s*$'
DBPEDIA_ONTOLOGY_PREFIX = "<http://dbpedia.org/ontology/"
RDFS_LABEL = f"<{str(RDFS.label)}>"


class rDF2:
    """
    RDF DataFrame wrapper for Spark.

    This class provides a wrapper around a Spark DataFrame that contains RDF triples.
    It provides methods for parsing, validating, and manipulating RDF data.

    Args:
        df: Spark DataFrame containing RDF triples.
    """
    def __init__(self, df: DataFrame):
        self.df = df
        self._validate(df)

    @staticmethod
    def _validate(df: DataFrame) -> None:
        expected = {"s", "p", "o", "isLiteral"}
        actual = set(df.columns)

        missing = expected - actual
        extra = actual - expected

        if missing or extra:
            parts = []
            if missing:
                parts.append(f"Missing columns: {sorted(missing)}")
            if extra:
                parts.append(f"Unexpected columns: {sorted(extra)}")
            raise ValueError("Schema validation failed. " + " | ".join(parts))

    @staticmethod
    def parse(spark, input_path) -> "rDF2":
        raw = spark.read.text(input_path)
        # Avoid catastrophic regex backtracking on large literals by tokenizing
        # into 3 parts: subject, predicate, object+dot (object may contain spaces).
        parts = F.split(F.col("line"), r"\s+", 3)
        parsed = (
            raw
            .select(F.col("value").alias("line"))
            .withColumn("line", F.trim(F.col("line")))
            .where(F.length(F.col("line")) > 0)
            .where(~F.col("line").startswith("#"))
            .where(F.col("line").rlike(r".*\.\s*$"))
            .where(F.size(parts) >= 3)
            .select(
                parts.getItem(0).alias("s"),
                parts.getItem(1).alias("p"),
                F.regexp_replace(parts.getItem(2), r"\s*\.\s*$", "").alias("o"),
            )
            .where(F.length(F.col("o")) > 0)
            .withColumn("isLiteral", F.col("o").startswith('"'))
        )
        return rDF2(parsed)

    @staticmethod
    def _type_filter_expr() -> F.Column:
        return (F.col("p") == f"<{str(RDF.type)}>") | (F.col("p") == F.lit("a"))

    @staticmethod
    def _schema_graph_property_filter_expr(property_filters: Iterable[str] | None) -> F.Column:
        filters = [f.strip() for f in (property_filters or []) if f and f.strip()]
        if not filters:
            return (F.col("p") == RDFS_LABEL) | (F.col("p").startswith(DBPEDIA_ONTOLOGY_PREFIX))

        expr = None
        for raw_filter in filters:
            if raw_filter.endswith("*"):
                token_expr = F.col("p").startswith(raw_filter[:-1])
            else:
                token_expr = F.col("p") == raw_filter
            expr = token_expr if expr is None else (expr | token_expr)
        return expr

    def serialize(self) -> DataFrame:
        return self.df.select(
            F.concat_ws(" ", "s", "p", "o", F.lit(".")).alias("value")
        )

    def write_nt(self, path):
        if os.path.exists(path):
            raise ValueError("path exisits " + path)

        self.serialize().write.text(path)

    # entity centric
    def filter_triples_by_s_type(self, o: str) -> "rDF2":
        df_types = (
            self.df
            .filter((F.col("p") == f"<{str(RDF.type)}>") & (F.col("o") == o))
            .select("s")
            .distinct()
        )

        filtered = (
            self.df.alias("d")
            .join(df_types.alias("t"), F.col("d.s") == F.col("t.s"), "inner")
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )

        return rDF2(filtered)

    def filter_triples_by_p_type(self, p: str) -> "rDF2":
        pass

    def filter_subgraph_by_entity_classes(self, classes: list[str]) -> "rDF2":
        """
        Keep only a class-scoped entity subgraph.

        Rules:
          1) Keep entities that have rdf:type in `classes`.
          2) Keep triples with subjects in that entity set where:
             - object is a literal, OR
             - object is also in that entity set, OR
             - triple is rdf:type and object is one of `classes`.
        """
        normalized_classes = [c.strip() for c in classes if c and c.strip()]
        if not normalized_classes:
            raise ValueError("classes must not be empty")

        spark = self.df.sparkSession
        allowed_classes = (
            spark.createDataFrame([(c,) for c in normalized_classes], "type string")
            .dropDuplicates(["type"])
            .cache()
        )

        entity_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
        )

        selected_entities = (
            entity_types.alias("et")
            .join(
                allowed_classes.alias("ac"),
                F.col("et.type") == F.col("ac.type"),
                "inner",
            )
            .select(F.col("et.entity").alias("entity"))
            .dropDuplicates(["entity"])
            .cache()
        )

        subject_scoped = (
            self.df.alias("d")
            .join(
                selected_entities.alias("se"),
                F.col("d.s") == F.col("se.entity"),
                "inner",
            )
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )

        literal_triples = subject_scoped.filter(F.col("isLiteral"))

        entity_to_entity_triples = (
            subject_scoped
            .filter(~F.col("isLiteral"))
            .join(
                selected_entities.alias("oe"),
                F.col("o") == F.col("oe.entity"),
                "inner",
            )
            .select("s", "p", "o", "isLiteral")
        )

        allowed_type_triples = (
            subject_scoped
            .filter(self._type_filter_expr())
            .join(
                allowed_classes.alias("ac"),
                F.col("o") == F.col("ac.type"),
                "inner",
            )
            .select("s", "p", "o", "isLiteral")
        )

        filtered = (
            literal_triples
            .unionByName(entity_to_entity_triples)
            .unionByName(allowed_type_triples)
            .dropDuplicates(["s", "p", "o", "isLiteral"])
        )

        return rDF2(filtered)

    def sample_entities_by_type_targets(
        self,
        type_targets: dict[str, int],
        related_per_seed: int = 5,
        seed: int = 13,
    ) -> "rDF2":
        """
        Build an entity-centric RDF sample that aims to meet per-type targets.

        Algorithm:
          1) Sort requested types by ascending global frequency (rarest first).
          2) For each type T, sample additional entities of T to satisfy target(T).
          3) For every newly sampled entity, also include up to N directly-related
             entities (resource-to-resource links only).
          4) Continue with the next type using the updated selected-entity set.
        """
        if not type_targets:
            raise ValueError("type_targets must not be empty")

        normalized_targets: dict[str, int] = {}
        for t, c in type_targets.items():
            if c < 0:
                raise ValueError(f"Target count must be >= 0 for type {t}")
            normalized_targets[t] = c

        spark = self.df.sparkSession
        selected = spark.createDataFrame([], "entity string")

        df_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
            .cache()
        )

        requested_type_df = spark.createDataFrame(
            [(t,) for t in normalized_targets.keys()],
            "type string",
        )

        type_order_rows = (
            df_types.groupBy("type")
            .count()
            .join(requested_type_df, on="type", how="inner")
            .orderBy(F.col("count").asc(), F.col("type").asc())
            .select("type")
            .collect()
        )
        type_order = [row["type"] for row in type_order_rows]

        if not type_order:
            return rDF2(self.df.limit(0))

        adjacency = (
            self.df
            .filter(~F.col("isLiteral"))
            .select(F.col("s").alias("src"), F.col("o").alias("dst"))
            .where(F.col("src") != F.col("dst"))
        )
        adjacency = (
            adjacency
            .unionByName(adjacency.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
            .dropDuplicates(["src", "dst"])
            .cache()
        )

        for idx, entity_type in enumerate(type_order):
            target = normalized_targets[entity_type]
            if target == 0:
                continue

            current_count = (
                selected.alias("sel")
                .join(
                    df_types.filter(F.col("type") == F.lit(entity_type)).alias("t"),
                    F.col("sel.entity") == F.col("t.entity"),
                    "inner",
                )
                .select(F.col("sel.entity"))
                .dropDuplicates(["entity"])
                .count()
            )

            missing = target - current_count
            if missing <= 0:
                continue

            candidates = (
                df_types
                .filter(F.col("type") == F.lit(entity_type))
                .select("entity")
                .join(selected, on="entity", how="left_anti")
                .dropDuplicates(["entity"])
            )

            sampled_t = candidates.orderBy(F.rand(seed + idx)).limit(missing)

            if related_per_seed > 0:
                neighbors = (
                    sampled_t.alias("seed")
                    .join(
                        adjacency.alias("adj"),
                        F.col("seed.entity") == F.col("adj.src"),
                        "inner",
                    )
                    .select(
                        F.col("seed.entity").alias("seed_entity"),
                        F.col("adj.dst").alias("entity"),
                    )
                )
                ranked_neighbors = neighbors.withColumn(
                    "rn",
                    F.row_number().over(
                        Window.partitionBy("seed_entity").orderBy(F.rand(seed + 1000 + idx))
                    ),
                )
                sampled_related = (
                    ranked_neighbors
                    .filter(F.col("rn") <= F.lit(related_per_seed))
                    .select("entity")
                )
                newly_added = sampled_t.unionByName(sampled_related).dropDuplicates(["entity"])
            else:
                newly_added = sampled_t

            selected = selected.unionByName(newly_added).dropDuplicates(["entity"])

        sampled_df = (
            self.df.alias("d")
            .join(selected.alias("sel"), F.col("d.s") == F.col("sel.entity"), "inner")
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )
        return rDF2(sampled_df)

    def sample_entities_global(
        self,
        sample_size: int,
        related_per_seed: int = 5,
        seed: int = 13,
    ) -> "rDF2":
        """
        Build an entity-centric RDF sample with a global entity budget.

        Picks up to `sample_size` unique subjects globally, then optionally adds
        up to `related_per_seed` directly related entities per sampled seed.
        """
        if sample_size < 0:
            raise ValueError("sample_size must be >= 0")
        if related_per_seed < 0:
            raise ValueError("related_per_seed must be >= 0")
        if sample_size == 0:
            return rDF2(self.df.limit(0))

        subjects = self.df.select(F.col("s").alias("entity")).dropDuplicates(["entity"])
        sampled = subjects.orderBy(F.rand(seed)).limit(sample_size)

        if related_per_seed > 0:
            adjacency = (
                self.df
                .filter(~F.col("isLiteral"))
                .select(F.col("s").alias("src"), F.col("o").alias("dst"))
                .where(F.col("src") != F.col("dst"))
            )
            adjacency = (
                adjacency
                .unionByName(adjacency.select(F.col("dst").alias("src"), F.col("src").alias("dst")))
                .dropDuplicates(["src", "dst"])
            )

            neighbors = (
                sampled.alias("seed")
                .join(
                    adjacency.alias("adj"),
                    F.col("seed.entity") == F.col("adj.src"),
                    "inner",
                )
                .select(
                    F.col("seed.entity").alias("seed_entity"),
                    F.col("adj.dst").alias("entity"),
                )
            )
            ranked_neighbors = neighbors.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("seed_entity").orderBy(F.rand(seed + 1000))
                ),
            )
            sampled_related = (
                ranked_neighbors
                .filter(F.col("rn") <= F.lit(related_per_seed))
                .select("entity")
            )
            selected = sampled.unionByName(sampled_related).dropDuplicates(["entity"])
        else:
            selected = sampled

        sampled_df = (
            self.df.alias("d")
            .join(selected.alias("sel"), F.col("d.s") == F.col("sel.entity"), "inner")
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )
        return rDF2(sampled_df)

    def sample_entities_all_types(
        self,
        target_per_type: int,
        related_per_seed: int = 5,
        seed: int = 13,
    ) -> "rDF2":
        """
        Build an entity-centric sample that targets every discovered rdf:type.

        For each discovered type T, tries to sample up to `target_per_type`
        entities of T (subject to availability), using rarity-first ordering and
        related-entity expansion.
        """
        if target_per_type < 0:
            raise ValueError("target_per_type must be >= 0")
        if target_per_type == 0:
            return rDF2(self.df.limit(0))
        if related_per_seed < 0:
            raise ValueError("related_per_seed must be >= 0")

        df_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
        )

        ranked_types = df_types.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("type").orderBy(F.rand(seed))
            ),
        )

        sampled_seeds = (
            ranked_types
            .filter(F.col("rn") <= F.lit(target_per_type))
            .select("entity")
            .dropDuplicates(["entity"])
        )

        if related_per_seed > 0:
            adjacency = (
                self.df
                .filter(~F.col("isLiteral"))
                .select(F.col("s").alias("src"), F.col("o").alias("dst"))
                .where(F.col("src") != F.col("dst"))
                .dropDuplicates(["src", "dst"])
            )

            neighbors = (
                sampled_seeds.alias("seed")
                .join(
                    adjacency.alias("adj"),
                    F.col("seed.entity") == F.col("adj.src"),
                    "inner",
                )
                .select(
                    F.col("seed.entity").alias("seed_entity"),
                    F.col("adj.dst").alias("entity"),
                )
            )

            ranked_neighbors = neighbors.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("seed_entity").orderBy(F.rand(seed + 1000))
                ),
            )
            sampled_related = (
                ranked_neighbors
                .filter(F.col("rn") <= F.lit(related_per_seed))
                .select("entity")
            )
            selected = sampled_seeds.unionByName(sampled_related).dropDuplicates(["entity"])
        else:
            selected = sampled_seeds

        sampled_df = (
            self.df.alias("d")
            .join(selected.alias("sel"), F.col("d.s") == F.col("sel.entity"), "inner")
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )
        return rDF2(sampled_df)

    def build_schema_graph_df(self, property_filters: Iterable[str] | None = None) -> DataFrame:
        """
        Build schema-level edge frequencies from triple-level RDF data.

        Produces columns: SourceType, Relation, TargetType, Count.
        """
        df_data = self.df.filter(self._schema_graph_property_filter_expr(property_filters))

        df_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
        )

        with_source = (
            df_data.alias("d")
            .join(df_types.alias("ts"), F.col("d.s") == F.col("ts.entity"), "inner")
            .select(
                F.col("d.p").alias("Relation"),
                F.col("d.o").alias("o"),
                F.col("d.isLiteral").alias("isLiteral"),
                F.col("ts.type").alias("SourceType"),
            )
        )

        non_literal_edges = (
            with_source
            .filter(~F.col("isLiteral"))
            .alias("x")
            .join(df_types.alias("to"), F.col("x.o") == F.col("to.entity"), "inner")
            .select(
                "SourceType",
                "Relation",
                F.col("to.type").alias("TargetType"),
            )
        )

        literal_edges = with_source.filter(F.col("isLiteral")).select(
            "SourceType",
            "Relation",
            F.lit("Literal").alias("TargetType"),
        )

        return (
            non_literal_edges
            .unionByName(literal_edges)
            .groupBy("SourceType", "Relation", "TargetType")
            .count()
            .withColumnRenamed("count", "Count")
            .orderBy(F.desc("Count"))
        )

    def write_schema_graph_csv(
        self,
        output_path: str,
        property_filters: Iterable[str] | None = None,
    ) -> None:
        """Write schema-graph aggregation as CSV with header."""
        (
            self.build_schema_graph_df(property_filters=property_filters)
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )


if __name__ == "__main__":

    spark = get_spark_session("SchemaGraphGenerator")

    (
        rDF2.parse(spark, "/config/workspace/vldb-data/endbpedia/selected.nt")
        .filter_triples_by_s_type("<http://dbpedia.org/ontology/Person>")
        .write_nt("/config/workspace/vldb-data/endbpedia/persons.nt")
    )

    spark.stop()