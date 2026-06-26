import os
from typing import Iterable, Literal

from pyodibel.management.spark_mgr import get_spark_session
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from rdflib import Graph, Namespace, RDF, RDFS

from pyodibel.operations.rdf.uris import normalize_class_uri as _normalize_class_uri

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
    def _type_predicate_on_p(p: F.Column) -> F.Column:
        """True when predicate is ``rdf:type`` or Turtle shortcut ``a``."""
        return (p == f"<{str(RDF.type)}>") | (p == F.lit("a"))

    @staticmethod
    def _type_filter_expr() -> F.Column:
        return rDF2._type_predicate_on_p(F.col("p"))

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

    def filter_triples_by_s_types(self, o: list[str], only_en: bool = False) -> "rDF2":

        df_types = (
            self.df
            .filter((F.col("p") == f"<{str(RDF.type)}>") & (F.col("o").isin(o)))
            .select("s")
            .distinct()
        )

        filtered = (
            self.df.alias("d")
            .join(df_types.alias("t"), F.col("d.s") == F.col("t.s"), "inner")
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )
        if only_en:
            filtered = filtered.filter(F.col("o").rlike(r'(@en$)|(^[^@]*$)'))

        return rDF2(filtered)

    def keep_triples_with_object_subject(self) -> "rDF2":
        """
        Keep triples whose non-literal object appears as a subject in the dataset,
        and keep all literal-object triples (literals are not subjects).

        Also keep ``rdf:type`` / ``a`` triples: class IRIs need not appear as subjects.

        Other non-literal edges to objects that never occur as ``s`` are dropped.
        """
        type_edge = self._type_predicate_on_p(F.col("d.p"))
        subjects = self.df.select(F.col("s").alias("subject")).dropDuplicates(["subject"])
        filtered = (
            self.df.alias("d")
            .join(subjects.alias("subj"), F.col("d.o") == F.col("subj.subject"), "left")
            .filter(F.col("d.isLiteral") | F.col("subj.subject").isNotNull() | type_edge)
            .select("d.s", "d.p", "d.o", "d.isLiteral")
        )
        return rDF2(filtered)


    def clean_rdf_types(self, o: list[str]) -> "rDF2":
        """
        For rdf:type only keep if is in the list of o
        For all other triples, keep
        """

        rdf_types = self.df.filter(F.col("p") == f"<{str(RDF.type)}>")
        other_triples = self.df.filter(F.col("p") != f"<{str(RDF.type)}>")

        rdf_types = rdf_types.filter(F.col("o").isin(o))

        return rDF2(rdf_types.unionByName(other_triples))


    def remove_duplicate_triples(self) -> "rDF2":
        """
        Remove all duplicate triples
        """
        cleaned_df = self.df.dropDuplicates()
        return rDF2(cleaned_df)

    def clean_domain_range_violations(self, ont_path) -> "rDF2":
        RDFS_NS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        RDFS_LITERAL = "http://www.w3.org/2000/01/rdf-schema#Literal"
        LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
        XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#"
        RDF_TYPE = f"<{str(RDF.type)}>"

        g = Graph()
        g.parse(ont_path, format="turtle")

        parents_map: dict[str, set[str]] = {}
        for s, _, o in g.triples((None, RDFS_NS.subClassOf, None)):
            parents_map.setdefault(str(s), set()).add(str(o))

        domains_map: dict[str, set[str]] = {}
        for s, _, o in g.triples((None, RDFS_NS.domain, None)):
            domains_map.setdefault(str(s), set()).add(str(o))

        ranges_map: dict[str, set[str]] = {}
        for s, _, o in g.triples((None, RDFS_NS.range, None)):
            ranges_map.setdefault(str(s), set()).add(str(o))

        object_types_map = (
            self.df
            .filter(F.col("p") == RDF_TYPE)
            .withColumn("type", F.regexp_extract("o", r"<(.+?)>", 1))
            .groupBy("s")
            .agg(F.collect_set("type").alias("types"))
            .rdd.map(lambda r: (r["s"], set(r["types"])))
            .collectAsMap()
        )

        def compute_ancestors(cls: str, parents: dict[str, set[str]]) -> frozenset:
            visited, queue = set(), [cls]
            while queue:
                cur = queue.pop()
                if cur in visited:
                    continue
                visited.add(cur)
                queue.extend(parents.get(cur, set()))
            return frozenset(visited)

        all_classes = set(parents_map.keys()) | {p for ps in parents_map.values() for p in ps}
        ancestors_map: dict[str, set[str]] = {
            cls: set(compute_ancestors(cls, parents_map))
            for cls in all_classes
        }

        spark = self.df.sparkSession
        bc_ancestors = spark.sparkContext.broadcast(ancestors_map)
        bc_domains = spark.sparkContext.broadcast(domains_map)
        bc_ranges = spark.sparkContext.broadcast(ranges_map)
        bc_object_types = spark.sparkContext.broadcast(object_types_map)

        subject_types_df = (
            self.df
            .filter(F.col("p") == RDF_TYPE)
            .withColumn("type", F.regexp_extract("o", r"<(.+?)>", 1))
            .groupBy("s")
            .agg(F.collect_set("type").alias("types"))
        )

        df = self.df.join(subject_types_df, on="s", how="left")

        def is_valid(p: str, o: str, types) -> bool:
            ancestors = bc_ancestors.value
            domains = bc_domains.value
            ranges = bc_ranges.value

            pred = p.strip("<>")
            subject_types = set(types) if types else set()

            pred_domains = domains.get(pred)
            if pred_domains:
                subject_ancestors = set()
                for t in subject_types:
                    subject_ancestors |= ancestors.get(t, {t})
                if not pred_domains & subject_ancestors:
                    return False

            pred_ranges = ranges.get(pred)
            if pred_ranges:
                if o.startswith("<"):
                    obj_iri = o.strip("<>")
                    obj_classes = bc_object_types.value.get(f"<{obj_iri}>", {obj_iri})
                    obj_ancestors = set()
                    for cls in obj_classes:
                        obj_ancestors |= ancestors.get(cls, {cls})
                    if not pred_ranges & obj_ancestors:
                        return False
                elif "^^<" in o:
                    datatype = o.split("^^<")[1].rstrip(">")
                    if not any(
                            r in (RDFS_LITERAL, datatype)
                            or (r.startswith(XSD_PREFIX) and datatype.startswith(XSD_PREFIX))
                            for r in pred_ranges
                    ):
                        return False
                else:
                    if not pred_ranges & {RDFS_LITERAL, LANG_STRING}:
                        return False

            return True

        is_valid_udf = F.udf(is_valid, BooleanType())

        filtered_df = (
            df
            .filter(is_valid_udf(F.col("p"), F.col("o"), F.col("types")))
            .drop("types")
        )

        return rDF2(filtered_df)

    def filter_triples_by_p_type(self, p: str) -> "rDF2":
        pass

    @staticmethod
    def normalize_class_uri(value: str) -> str:
        return _normalize_class_uri(value)

    def _induce_subgraph_from_entities(
        self,
        selected_entities: DataFrame,
        *,
        allowed_type_objects: Iterable[str] | None = None,
    ) -> "rDF2":
        selected_entities = (
            selected_entities
            .select(F.col("entity").alias("entity"))
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

        type_triples = subject_scoped.filter(self._type_filter_expr())
        if allowed_type_objects is not None:
            allowed = [t.strip() for t in allowed_type_objects if t and t.strip()]
            allowed_df = (
                self.df.sparkSession
                .createDataFrame([(t,) for t in allowed], "type string")
                .dropDuplicates(["type"])
            )
            type_triples = (
                type_triples
                .join(allowed_df.alias("ac"), F.col("o") == F.col("ac.type"), "inner")
                .select("s", "p", "o", "isLiteral")
            )

        filtered = (
            literal_triples
            .unionByName(entity_to_entity_triples)
            .unionByName(type_triples)
            .dropDuplicates(["s", "p", "o", "isLiteral"])
        )
        selected_entities.unpersist()
        return rDF2(filtered)

    def filter_reachable_from_classes(
        self,
        root_classes: str | list[str],
        *,
        max_hops: int | None = None,
        direction: Literal["forward", "backward", "both"] = "forward",
    ) -> "rDF2":
        """
        Keep the entity subgraph reachable from entities of the given root class(es).

        Seeds are entities with ``rdf:type`` in ``root_classes``. The reachable set
        is expanded by following non-literal resource edges (forward along subject
        to object by default), then all triples whose subject is reachable are kept.
        """
        if isinstance(root_classes, str):
            normalized_classes = [self.normalize_class_uri(root_classes)]
        else:
            normalized_classes = [self.normalize_class_uri(c) for c in root_classes]
        if not normalized_classes:
            raise ValueError("root_classes must not be empty")
        if max_hops is not None and max_hops < 0:
            raise ValueError("max_hops must be >= 0")

        entity_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
        )

        seeds = (
            entity_types
            .filter(F.col("type").isin(normalized_classes))
            .select("entity")
            .dropDuplicates(["entity"])
        )

        if seeds.limit(1).count() == 0:
            return rDF2(self.df.limit(0))

        forward_edges = (
            self.df
            .filter(~F.col("isLiteral"))
            .select(F.col("s").alias("src"), F.col("o").alias("dst"))
            .where(F.col("src") != F.col("dst"))
            .dropDuplicates(["src", "dst"])
            .cache()
        )

        reachable = seeds.cache()
        hop = 0
        while True:
            if max_hops is not None and hop >= max_hops:
                break

            neighbor_parts = []
            if direction in ("forward", "both"):
                neighbor_parts.append(
                    forward_edges.alias("e")
                    .join(reachable.alias("r"), F.col("e.src") == F.col("r.entity"), "inner")
                    .select(F.col("e.dst").alias("entity"))
                )
            if direction in ("backward", "both"):
                neighbor_parts.append(
                    forward_edges.alias("e")
                    .join(reachable.alias("r"), F.col("e.dst") == F.col("r.entity"), "inner")
                    .select(F.col("e.src").alias("entity"))
                )

            neighbors = neighbor_parts[0]
            for part in neighbor_parts[1:]:
                neighbors = neighbors.unionByName(part)
            neighbors = neighbors.dropDuplicates(["entity"])

            new_entities = neighbors.join(reachable, on="entity", how="left_anti")
            if new_entities.limit(1).count() == 0:
                break

            previous = reachable
            reachable = reachable.unionByName(new_entities).dropDuplicates(["entity"]).cache()
            previous.unpersist()
            hop += 1

        forward_edges.unpersist()
        result = self._induce_subgraph_from_entities(reachable, allowed_type_objects=None)
        reachable.unpersist()
        return result

    def filter_reachable_from_entities(
        self,
        seeds: str | list[str],
        *,
        max_hops: int | None = None,
        direction: Literal["forward", "backward", "both"] = "forward",
        main_class: str | None = None,
        main_class_scope: Literal["reachable", "seeds"] = "reachable",
    ) -> "rDF2":
        """
        Keep the entity subgraph reachable from explicit seed entity URIs.

        Seeds are given directly (not resolved via rdf:type). The reachable set
        is expanded by following non-literal resource edges, then all triples whose
        subject is reachable are kept.

        When ``main_class_scope`` is ``seeds``, reachable entities typed as
        ``main_class`` are limited to the seed set; other classes still expand
        normally.
        """
        if main_class_scope == "seeds" and main_class is None:
            raise ValueError("main_class is required when main_class_scope is 'seeds'")
        if isinstance(seeds, str):
            seed_list = [seeds]
        else:
            seed_list = list(seeds)
        if not seed_list:
            raise ValueError("seeds must not be empty")
        if max_hops is not None and max_hops < 0:
            raise ValueError("max_hops must be >= 0")

        seeds_df = (
            self.df.sparkSession
            .createDataFrame([(entity,) for entity in seed_list], "entity string")
            .dropDuplicates(["entity"])
        )

        if seeds_df.limit(1).count() == 0:
            return rDF2(self.df.limit(0))

        forward_edges = (
            self.df
            .filter(~F.col("isLiteral"))
            .select(F.col("s").alias("src"), F.col("o").alias("dst"))
            .where(F.col("src") != F.col("dst"))
            .dropDuplicates(["src", "dst"])
            .cache()
        )

        reachable = seeds_df.cache()
        hop = 0
        while True:
            if max_hops is not None and hop >= max_hops:
                break

            neighbor_parts = []
            if direction in ("forward", "both"):
                neighbor_parts.append(
                    forward_edges.alias("e")
                    .join(reachable.alias("r"), F.col("e.src") == F.col("r.entity"), "inner")
                    .select(F.col("e.dst").alias("entity"))
                )
            if direction in ("backward", "both"):
                neighbor_parts.append(
                    forward_edges.alias("e")
                    .join(reachable.alias("r"), F.col("e.dst") == F.col("r.entity"), "inner")
                    .select(F.col("e.src").alias("entity"))
                )

            neighbors = neighbor_parts[0]
            for part in neighbor_parts[1:]:
                neighbors = neighbors.unionByName(part)
            neighbors = neighbors.dropDuplicates(["entity"])

            new_entities = neighbors.join(reachable, on="entity", how="left_anti")
            if new_entities.limit(1).count() == 0:
                break

            previous = reachable
            reachable = reachable.unionByName(new_entities).dropDuplicates(["entity"]).cache()
            previous.unpersist()
            hop += 1

        forward_edges.unpersist()

        if main_class_scope == "seeds":
            normalized_class = self.normalize_class_uri(main_class)
            type_expr = self._type_filter_expr()
            non_seed_main_class = (
                self.df
                .filter(type_expr & (F.col("o") == normalized_class))
                .select(F.col("s").alias("entity"))
                .dropDuplicates(["entity"])
                .join(seeds_df, on="entity", how="left_anti")
            )
            reachable = reachable.join(non_seed_main_class, on="entity", how="left_anti")

        result = self._induce_subgraph_from_entities(reachable, allowed_type_objects=None)
        reachable.unpersist()
        return result

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

        entity_types = (
            self.df
            .filter(self._type_filter_expr())
            .select(F.col("s").alias("entity"), F.col("o").alias("type"))
            .dropDuplicates(["entity", "type"])
        )

        allowed_classes = (
            self.df.sparkSession
            .createDataFrame([(c,) for c in normalized_classes], "type string")
            .dropDuplicates(["type"])
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
        )

        return self._induce_subgraph_from_entities(
            selected_entities,
            allowed_type_objects=normalized_classes,
        )

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

    def property_filter(self, property_filters: Iterable[str] | None = None) -> "rDF2":
        df_data = self.df.filter(self._schema_graph_property_filter_expr(property_filters))
        return rDF2(df_data)

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

        # Left joins: keep edges whose subject/object has no rdf:type (label as Untyped).
        with_source = (
            df_data.alias("d")
            .join(df_types.alias("ts"), F.col("d.s") == F.col("ts.entity"), "left")
            .select(
                F.col("d.p").alias("Relation"),
                F.col("d.o").alias("o"),
                F.col("d.isLiteral").alias("isLiteral"),
                F.coalesce(F.col("ts.type"), F.lit("Untyped")).alias("SourceType"),
            )
        )

        non_literal_edges = (
            with_source
            .filter(~F.col("isLiteral"))
            .alias("x")
            .join(df_types.alias("to"), F.col("x.o") == F.col("to.entity"), "left")
            .select(
                "SourceType",
                "Relation",
                F.coalesce(F.col("to.type"), F.lit("Untyped")).alias("TargetType"),
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
    def build_schema_graph_100_df(self, property_filters: Iterable[str] | None = None) -> DataFrame:
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

        # Left joins: keep edges whose subject/object has no rdf:type (label as Untyped).
        with_source = (
            df_data.alias("d")
            .join(df_types.alias("ts"), F.col("d.s") == F.col("ts.entity"), "left")
            .select(
                F.col("d.p").alias("Relation"),
                F.col("d.o").alias("o"),
                F.col("d.isLiteral").alias("isLiteral"),
                F.coalesce(F.col("ts.type"), F.lit("Untyped")).alias("SourceType"),
            )
        )

        non_literal_edges = (
            with_source
            .filter(~F.col("isLiteral"))
            .alias("x")
            .join(df_types.alias("to"), F.col("x.o") == F.col("to.entity"), "left")
            .select(
                "SourceType",
                "Relation",
                F.coalesce(F.col("to.type"), F.lit("Untyped")).alias("TargetType"),
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
            .filter(F.col("count") >= 100)
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