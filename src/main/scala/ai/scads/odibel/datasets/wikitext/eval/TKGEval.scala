package ai.scads.odibel.datasets.wikitext.eval


import org.apache.spark.sql.SparkSession


object TKGEval extends App {

  // Input and Output file path passed as an argument
  val in = args(0)
  val out = args(1)

  // Create Spark Session
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sql = spark.sqlContext
  import sql.implicits._

  // Read the JSON files and convert to Dataset[CSVRow]
  val data = sql.read.json(in)
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .as[CSVRow]
  data.show(3)
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // 1. Calculate all unique windows
  val uniqueWindows = TKGUtils.countAllUniqueWindows(data)
  println(s"All unique windows: $uniqueWindows")
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // 2. Count triples per subject
  val triplesPerSubject = TKGUtils.countTriplesPerSubject(data)
  println("Count triples per subject: ")
  triplesPerSubject.show(5)
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // 3. Revisions per page
  val revisionsPerPage = TKGUtils.countRevisionsPerPage(data)
  println("Revisions per page: ")
  revisionsPerPage.show(5)
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // 4. Changes per predicate
  val changesPerPredicate = TKGUtils.countChangesPerPredicate(data)
  println("Changes per predicate: ")
  changesPerPredicate.show(5)
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // 5. Create snapshot
  val timestamp = 1094215048000L
  val snapshot = TKGUtils.createSnapshot(data=data, timestamp=timestamp, outputPath=Some(out))
  println("Create snapshot: ")
  snapshot.show(5)
  println("________________________________________________________________________")
  println("________________________________________________________________________\n")

  // Stop SparkSession
  spark.stop()
}
