import os
import sys

from dotenv import load_dotenv
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2
from pyspark.sql import functions as F


RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def compute_and_write_stats(input_path: str) -> None:

    spark = get_spark_session("CrossMultiSourceKGGenerator")

    rdf = rDF2.parse(spark, input_path)
    df = rdf.df.select("s", "p", "o").persist()

    stats_path = os.path.join(os.path.dirname(input_path), "stats")

    # Triple count
    triple_count_df = df.agg(F.count("*").alias("triple_count"))
    triple_count_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(stats_path, "triple_count.csv"), header=True
    )

    # Top-1000 subject / predicate / object counts
    for col_name in ("s", "p", "o"):
        top = (
            df.rdd
            .map(lambda row, c=col_name: (row[c], 1))
            .reduceByKey(lambda a, b: a + b)
            .map(lambda x: (x[1], x[0]))
            .top(1000)
        )
        spark.createDataFrame([(v, c) for c, v in top], [col_name, "count"]) \
             .coalesce(1).write.mode("overwrite") \
             .option("header", "true") \
             .csv(os.path.join(stats_path, f"{col_name}_counts_top1000.csv"))

    # Distinct entity count (subjects)
    distinct_entity_df = (
        df.select("s").distinct()
        .agg(F.count("*").alias("distinct_entity_count"))
    )
    distinct_entity_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(stats_path, "distinct_entity_count.csv"))

    # Distinct predicates
    property_count_df = (
        df.select("p").distinct()
        .agg(F.count("*").alias("property_count"))
    )
    property_count_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(stats_path, "property_count.csv"))

    # Class count + class occurrence
    type_df = df.filter(F.col("p") == RDF_TYPE).select(
        F.col("s").alias("entity"),
        F.col("o").alias("class"),
    ).persist()

    # distinct classes
    class_count_df = (
        type_df.select("class").distinct()
        .agg(F.count("*").alias("class_count"))
    )
    class_count_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(stats_path, "class_count.csv"))

    # class occurrence
    class_occurrence_df = (
        type_df.groupBy("class")
        .agg(F.count("entity").alias("entity_count"))
        .orderBy(F.desc("entity_count"))
    )
    class_occurrence_df.coalesce(1).write.mode("overwrite") \
        .option("header", True) \
        .csv(os.path.join(stats_path, "class_occurrence.csv"))

    type_df.unpersist()

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: statistics.py <input_path>")
        sys.exit(1)

    compute_and_write_stats(sys.argv[1])