"""
Generate a Knowledge Graph from Gradoop/FAMER entity resolution data using Spark.

Auto-derives ontology from property names and sources, outputs Turtle format.
"""

import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, lit, first, udf
from pyspark.sql.types import StructType, StructField, StringType, MapType

from pyodibel.api.source import SourceConfig
from pyodibel.source.gradoop.source import GradoopSource


def normalize_name(name: str) -> str:
    """Normalize a property/class name to a valid URI local name."""
    name = name.strip("<>")
    name = re.sub(r"[^a-zA-Z0-9\s]", " ", name)
    words = name.split()
    if not words:
        return "unknown"
    return words[0].lower() + "".join(w.title() for w in words[1:])


def source_to_class_name(source: str) -> str:
    """Convert source name to a class name."""
    parts = source.replace("www.", "").replace(".", " ").split()
    return "".join(p.title() for p in parts)


def generate_ontology_ttl(sources: list, properties: list) -> str:
    """Generate ontology as Turtle string."""
    lines = [
        "@prefix dexter: <http://example.org/dexter/> .",
        "@prefix dexterProp: <http://example.org/dexter/property/> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
        "# Base Entity class",
        "dexter:Entity a owl:Class ;",
        '    rdfs:label "Entity" .',
        "",
        "# Source classes",
    ]
    
    for src in sources:
        class_name = source_to_class_name(src)
        lines.extend([
            f"dexter:{class_name} a owl:Class ;",
            f'    rdfs:label "{src}" ;',
            f'    rdfs:comment "Entities from {src}" ;',
            "    rdfs:subClassOf dexter:Entity .",
            "",
        ])
    
    lines.append("# Properties")
    for prop in properties:
        prop_name = normalize_name(prop)
        lines.extend([
            f"dexterProp:{prop_name} a owl:DatatypeProperty ;",
            f'    rdfs:label "{prop}" ;',
            "    rdfs:domain dexter:Entity ;",
            "    rdfs:range xsd:string .",
            "",
        ])
    
    return "\n".join(lines)


def entity_to_ttl(entity_id: str, source: str, cluster_id: str, properties: dict) -> str:
    """Convert a single entity to Turtle triples."""
    class_name = source_to_class_name(source)
    lines = [f"dexterEnt:{entity_id} a dexter:{class_name}"]
    
    for prop_key, prop_value in properties.items():
        if prop_value is not None:
            prop_name = normalize_name(prop_key)
            # Escape quotes in string values
            escaped_value = str(prop_value).replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'    dexterProp:{prop_name} "{escaped_value}"')
    
    return " ;\n".join(lines) + " ."


def entity_to_ntriples(entity_id: str, source: str, cluster_id: str, properties: dict) -> str:
    """Convert a single entity to N-Triples string."""
    class_name = source_to_class_name(source)
    lines = [f"<{entity_id}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{class_name}>"]
    
    for prop_key, prop_value in properties.items():
        if prop_value is not None:
            prop_name = normalize_name(prop_key)
            lines.append(f'<{entity_id}> <http://example.org/dexter/property/{prop_name}> "{prop_value}"')

    return " .\n".join(lines)+" ."

def main():
    # Configuration
    input_folder = "/d/repo/MA Lerm/Datensammlungen/DS-C/DS-C0/SW_0.7"
    output_dir = Path("/home/marvin/phd/odibel/output")
    output_dir.mkdir(exist_ok=True)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DexterKGGenerator") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load entities via GradoopSource
    config = SourceConfig(
        name="dexter-kg-ds-c0",
        source_type="gradoop",
        location=input_folder
    )
    source = GradoopSource(config)
    
    print(f"Loading entities from: {input_folder}")
    entities = list(source.read_entities())
    clusters = source.get_clusters()
    print(f"Loaded {len(entities)} entities, {len(clusters)} clusters")
    
    # Convert to Spark DataFrame
    entity_data = [
        (e.id, e.source, e.cluster_id, e.properties)
        for e in entities
    ]
    
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("source", StringType(), False),
        StructField("cluster_id", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ])
    
    df = spark.createDataFrame(entity_data, schema)
    # df.show()
    df.cache()
    
    print(f"Created DataFrame with {df.count()} rows")
    
    # Collect unique sources and properties
    sources = [row.source for row in df.select("source").distinct().collect()]
    
    # Collect all property keys
    all_properties = set()
    for e in entities:
        all_properties.update(e.properties.keys())
    all_properties = list(all_properties)
    
    print(f"Found {len(sources)} sources, {len(all_properties)} properties")
    
    # Generate and save ontology
    ontology_ttl = generate_ontology_ttl(sources, all_properties)
    onto_path = output_dir / "dexter_ontology.ttl"
    with open(onto_path, "w") as f:
        f.write(ontology_ttl)
    print(f"Saved ontology to: {onto_path}")
    
    


    df.rdd.map(lambda row: entity_to_ntriples(row.id, row.source, row.cluster_id, row.properties)).saveAsTextFile((output_dir / "dexter_entities.nt").as_posix())
    print(f"Saved entities to: {output_dir / 'dexter_entities.nt'}")
    
    # print(f"{'='*60}")
    # print(f"RDF generation complete!")
    # print(f"Output directory: {output_dir.absolute()}")
    # print(f"Generated {len(graphs_by_source)} source graphs + 1 combined graph")
    # print(f"{'='*60}")


if __name__ == "__main__":
    main()
