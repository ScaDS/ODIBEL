package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.io.File
import java.time.Duration

// TODO call this by class using picocli
object PropertyEval extends App {

  val sql = SparkSessionUtil.sql

  def writeOut(name: String, ds: DataFrame): Unit = {
    ds.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(new File("/user/hofer/dbpedia-tkg/stats/FULL/", name).getPath)
  }

  val start = System.nanoTime()

  val df = sql.read.parquet("/user/hofer/dbpedia-tkg/tmp/tkg_all_20240601.parquet")

  // Step 1: Count distinct windows and compute "changes - 1" per head-rel
  val changesDF = df.select("head", "rel", "tFrom", "tUntil").groupBy("head", "rel")
    .agg((countDistinct("tFrom", "tUntil") - 1).alias("changes"))

  // Step 2: Count the total number of distinct heads for each relation
  val totalHeadsPerRel = changesDF.select("rel","changes")
    .groupBy("rel")
    .agg(sum("changes").alias("total_changes"))

  // Step 3: Compute aggregate statistics for each relation
  val statsDF = changesDF.groupBy("rel")
    .agg(
      avg("changes").alias("avg_changes"),
      min("changes").alias("min_changes"),
      max("changes").alias("max_changes"),
      call_udf("percentile_approx", col("changes"), lit(0.25)).alias("25th_percentile"),
      call_udf("percentile_approx", col("changes"), lit(0.5)).alias("median"),
      call_udf("percentile_approx", col("changes"), lit(0.75)).alias("75th_percentile")
    )

  val results = statsDF.join(totalHeadsPerRel, "rel").filter(!col("rel").startsWith("http://dbpedia.org/property")).orderBy(desc("avg_changes"))

  writeOut("propertyChangesAll", results)

  println(Duration.ofNanos(System.nanoTime() - start))
  // PT30M37.772128091S on AMD Ryzen 9 7945HX for full extraction
}