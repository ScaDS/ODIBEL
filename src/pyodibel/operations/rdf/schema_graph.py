import click
from pyspark.sql import SparkSession, functions as F
from rdflib import RDF, RDFS

NT_RE = r'^\s*(<[^>]*>|_:[A-Za-z0-9_]+|[^\s]+)\s+' \
        r'(<[^>]*>|[^\s]+)\s+' \
        r'((?:"(?:\\.|[^"\\])*"(?:@[A-Za-z-]+|\^\^<[^>]*>|\^\^[^\s]+)?)|(?:<[^>]*>|_:[A-Za-z0-9_]+|[^\s]+))\s*\.\s*$'


def build_schema_graph(spark, input_path, output_path):
    # ---------
    # 1) Read + parse N-Triples
    # ---------
    raw = spark.read.text(input_path)

    parsed = (
        raw
        .select(F.col("value").alias("line"))
        .where(F.length(F.trim("line")) > 0)
        .where(F.col("line").rlike(NT_RE))
        .select(
            F.regexp_extract("line", NT_RE, 1).alias("s"),
            F.regexp_extract("line", NT_RE, 2).alias("p"),
            F.regexp_extract("line", NT_RE, 3).alias("o"),
            F.regexp_extract("line", NT_RE, 3).startswith('"').alias("isLiteral")
        )
    )

    # ---------
    # 2) Filter #1: data triples
    # ---------
    df_data = parsed.filter(
        (F.col("p").startswith("<"+str(RDFS.label))) | (F.col("p").startswith("<http://dbpedia.org/ontology"))
    )

    # ---------
    # 3) Filter #2: type triples
    # ---------
    df_types = (
        parsed
        .filter((F.col("p").startswith("<"+str(RDF.type))) | (F.col("p") == F.lit("a")))
        .select(F.col("s").alias("entity"), F.col("o").alias("type"))
        .dropDuplicates(["entity", "type"])
    )

    # # Broadcast for efficiency (usually small)
    # df_types = F.broadcast(df_types)

    # ---------
    # 4) Join subject -> sourceType
    # ---------
    with_source = (
        df_data.alias("d")
        .join(df_types.alias("ts"), F.col("d.s") == F.col("ts.entity"), "inner")
        .select(
            F.col("d.s").alias("s"),
            F.col("d.p").alias("Relation"),
            F.col("d.o").alias("o"),
            F.col("d.isLiteral").alias("isLiteral"),
            F.col("ts.type").alias("SourceType")
        )
    )

    # ---------
    # 5) Join object -> targetType
    # ---------
    non_lit = with_source.filter(~F.col("isLiteral"))
    lit = with_source.filter(F.col("isLiteral"))

    non_lit_joined = (
        non_lit.alias("x")
        .join(df_types.alias("to"), F.col("x.o") == F.col("to.entity"), "inner")
        .select(
            "SourceType",
            "Relation",
            F.col("to.type").alias("TargetType")
        )
    )

    lit_mapped = (
        lit.select(
            "SourceType",
            "Relation",
            F.lit("Literal").alias("TargetType")
        )
    )

    edges = non_lit_joined.unionByName(lit_mapped)

    # ---------
    # 6) groupBy / count
    # ---------
    result = (
        edges
        .groupBy("SourceType", "Relation", "TargetType")
        .count()
        .withColumnRenamed("count", "Count")
        .orderBy(F.desc("Count"))
    )

    # ---------
    # 7) Write output
    # ---------
    (
        result
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )


@click.command()
@click.option("--input", "input_path", required=True, type=click.Path(exists=True))
@click.option("--output", "output_path", required=True, type=click.Path())
@click.option("--master", default="local[*]", show_default=True, help="Spark master, e.g. local[*] or spark://HOST:7077")
@click.option("--driver-memory", default="8g", show_default=True)
@click.option("--executor-memory", default=None, show_default=True, help="Only used for cluster masters (ignored in pure local mode)")
@click.option("--shuffle-partitions", default=2000, show_default=True, type=int)
@click.option("--local-dir", default="/tmp/spark", show_default=True, help="Local spill/shuffle directory")
def cli(input_path, output_path, master, driver_memory, executor_memory, shuffle_partitions, local_dir):
    builder = (
        SparkSession.builder
        .appName("SchemaGraphGenerator")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.local.dir", local_dir)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
    )
    
    # In cluster mode, this matters. In local mode, it's effectively irrelevant.
    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)

    spark = builder.getOrCreate()
    build_schema_graph(spark, input_path, output_path)
    spark.stop()


if __name__ == "__main__":
    cli()
