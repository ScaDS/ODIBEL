package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.eval.CSVRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TKGEval extends App {

  // Input and Output file path passed as an argument
  val in = args(0)
  val out = args(1)

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sql = spark.sqlContext
  import sql.implicits._

  // Read the JSON files and convert to Dataset[CSVRow]
  val data = sql.read.json(in)
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .as[CSVRow]
  // .select("rel").groupBy("rel").count().show(false)
  data.show(3)

  // 1. Calculate all unique windows
  val uniqueWindows = TKGUtils.calculateAllUniqueWindows(data)
  println(s"All unique windows: $uniqueWindows")
  println("_____________")

  // 2. Count triples per subject
  val triplesPerSubject = TKGUtils.countTriplesPerSubject(data)
  println("Triples per subject: ")
  triplesPerSubject.show(false)
  println("_____________")

  // 3. Revisions per page
  val revisionsPerPage = TKGUtils.calculateRevisionsPerPage(data)
  revisionsPerPage.show(false)
  println("_____________")

  // 4. Changes per predicate
  val changesPerPredicate = TKGUtils.calculateChangesPerPredicate(data)
  changesPerPredicate.show(false)
  println("_____________")

  // 5. Create snapshot
  val timestamp = 1094215048000L
  val snapshot = TKGUtils.createSnapshot(data=data, timestamp=timestamp, outputPath=Some(out))
  snapshot.show(3)
  println("_____________")

  // Stop SparkSession
  spark.stop()
}
