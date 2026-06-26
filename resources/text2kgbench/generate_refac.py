"""
Generate Text2KGBench subgraphs from a DBpedia dump, driven by a Text2KGBench ontology TTL.

Classes and properties are extracted from the ontology file with rdflib instead of
being hardcoded in per-ontology scripts.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF

from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2

DBO_NS = "http://dbpedia.org/ontology/"
RDF_TYPE_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
RDFS_LABEL_URI = "<http://www.w3.org/2000/01/rdf-schema#label>"

OWL_PROPERTY_TYPES = (OWL.ObjectProperty, OWL.DatatypeProperty, RDF.Property)

def _is_remote_path(path: str) -> bool:
    return "://" in path and not path.startswith("file://")


def _local_path(path: str) -> str:
    return path[7:] if path.startswith("file://") else path


def _hadoop_path_exists(spark, path: str) -> bool:
    """
    Check existence using Hadoop FS (supports hdfs://, s3a://, etc.).

    Uses Spark's JVM bridge to query org.apache.hadoop.fs.FileSystem.
    """
    jvm = spark.sparkContext._jvm
    jconf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(jconf)
    return fs.exists(hadoop_path)


def _path_exists(spark, path: str) -> bool:
    if _is_remote_path(path):
        return _hadoop_path_exists(spark, path)
    return os.path.exists(_local_path(path))


def _assert_ontology_path(ontology_path: str) -> None:
    if _is_remote_path(ontology_path):
        return
    if not os.path.isfile(_local_path(ontology_path)):
        raise FileNotFoundError(f"Ontology file not found: {ontology_path}")


def _load_ontology_text(ontology_path: str, spark) -> str:
    if _is_remote_path(ontology_path):
        # Reading via Spark works for HDFS/S3/etc, but avoid doing this for huge ontologies.
        rows = spark.read.text(ontology_path).collect()
        return "\n".join(row.value for row in rows)
    with open(_local_path(ontology_path), encoding="utf-8") as handle:
        return handle.read()


def _resolve_dbo_uri(graph: Graph, subject: URIRef, equivalent_predicate) -> str | None:
    for equivalent in graph.objects(subject, equivalent_predicate):
        if isinstance(equivalent, URIRef) and str(equivalent).startswith(DBO_NS):
            return f"<{equivalent}>"
    if str(subject).startswith(DBO_NS):
        return f"<{subject}>"
    return None


def extract_from_ontology(ontology_path: str, *, spark=None) -> tuple[list[str], list[str]]:
    graph = Graph()
    if _is_remote_path(ontology_path):
        if spark is None:
            raise ValueError(f"Spark session required to read ontology: {ontology_path}")
        graph.parse(data=_load_ontology_text(ontology_path, spark), format="turtle")
    else:
        graph.parse(_local_path(ontology_path), format="turtle")

    classes: set[str] = set()
    for subject in graph.subjects(RDF.type, OWL.Class):
        if not isinstance(subject, URIRef):
            continue
        uri = _resolve_dbo_uri(graph, subject, OWL.equivalentClass)
        if uri:
            classes.add(uri)

    properties: set[str] = set()
    for property_type in OWL_PROPERTY_TYPES:
        for subject in graph.subjects(RDF.type, property_type):
            if not isinstance(subject, URIRef):
                continue
            uri = _resolve_dbo_uri(graph, subject, OWL.equivalentProperty)
            if uri:
                properties.add(uri)

    return sorted(classes), sorted(properties)


def ontology_name_from_path(ontology_path: str) -> str:
    """Derive output prefix like 'ont_13' from '13_food_ontology.ttl'."""
    stem = Path(ontology_path.rstrip("/").rsplit("/", 1)[-1]).stem
    match = re.match(r"(\d+)_", stem)
    if match:
        return f"ont_{match.group(1)}"
    return stem.replace("_ontology", "")


def generate(
    input_path: str,
    output_path: str,
    ontology_path: str,
    name: str | None = None,
    master: str | None = None,
):
    spark = get_spark_session("CrossMultiSourceKGGenerator", master=master)
    classes, properties = extract_from_ontology(ontology_path, spark=spark)
    if not classes:
        raise ValueError(f"No owl:Class entries found in ontology: {ontology_path}")
    if not properties:
        raise ValueError(f"No owl properties found in ontology: {ontology_path}")

    properties = list(properties)
    properties.append(RDF_TYPE_URI)
    properties.append(RDFS_LABEL_URI)

    if name is None:
        name = ontology_name_from_path(ontology_path)

    print(f"Ontology: {ontology_path}")
    print(f"Output prefix: {name}")
    print(f"Classes ({len(classes)}): {', '.join(classes)}")
    print(f"Properties ({len(properties)}): {', '.join(properties)}")

    property_filter_path = os.path.join(output_path, f"{name}_subgraph/property_filter")

    print("Filtering properties...")
    if not os.path.exists(property_filter_path): #TODO hdfs or s3
        (
            rDF2.parse(spark, input_path)
            .property_filter(properties)
            .filter_subgraph_by_entity_classes(classes)
            .write_nt(property_filter_path)
        )
    else:
        print(f"Skipping existing output: {property_filter_path}")

    schema_graph_path = os.path.join(output_path, f"{name}_subgraph/property_filter_schema")

    if not _path_exists(spark, schema_graph_path):
        (
            rDF2.parse(spark, property_filter_path)
            .build_schema_graph_df(properties)
            .coalesce(1)
            .write.csv(schema_graph_path, header=True)
        )
    else:
        print(f"Skipping existing output: {schema_graph_path}")

    spark.stop()


def main(argv: list[str] | None = None) -> None:
    argv = argv if argv is not None else sys.argv[1:]

    if len(argv) not in (3, 4):
        print(
            "Usage: generate_refac.py <input_path> <output_path> <ontology_path> [name_prefix]"
        )
        sys.exit(1)

    input_path, output_path, ontology_path = argv[:3]
    name = argv[3] if len(argv) == 4 else None

    _assert_ontology_path(ontology_path)

    if not input_path or not output_path:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))

    if not input_path:
        input_path = os.getenv("INPUT_PATH")
        if not input_path:
            raise ValueError("INPUT_PATH not set in .env file")

    if not output_path:
        output_path = os.getenv("OUTPUT_PATH")
        if not output_path:
            raise ValueError("OUTPUT_PATH not set in .env file")

    generate(input_path, output_path, ontology_path, name=name)


if __name__ == "__main__":
    main()
