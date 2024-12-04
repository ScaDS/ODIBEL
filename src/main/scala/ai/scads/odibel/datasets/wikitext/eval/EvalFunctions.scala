package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.TemporalExtractionResult
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


object EvalFunctions {

  //   Function: Calculate all unique windows
  def countAllUniqueWindows(data: Dataset[TemporalExtractionResult]): Long = {
    import data.sparkSession.implicits._
    data.map(row => row.tFrom.toString + row.tUntil.toString: String).distinct().count()
  }

  // Function: Count triples per subject
  def countTriplesPerSubject(data: Dataset[TemporalExtractionResult]): DataFrame = {
    data.select("head", "rel", "tail")
      .orderBy("head", "rel", "tail").distinct()
      .groupBy("head")
      .agg(
        countDistinct("*").as("triple_count") // Count unique triples per subject
      )
      .orderBy("head")
  }

    // Function: Revisions per page
    def countRevisionsPerPage(data: Dataset[TemporalExtractionResult]): DataFrame = {
      import data.sparkSession.implicits._

      // 1. Filter pages with wikiPageID relation and rename 'head' to 'page'
      val pages = data.filter(_.rel == "http://dbpedia.org/ontology/wikiPageID")
        .map(row => row.head) // Extract the 'head' field
        .distinct() // Remove duplicates
        .toDF("page") // Convert to DataFrame and rename column to 'page'

      // 2. Join the pages with the original dataset to get revision intervals
      val revisions = data.toDF().join(pages, data("head") === pages("page"))
        .select(data("head"), data("rFrom"), data("rUntil")) // Select relevant columns

      // 3. Group by page (head) and count the number of revisions
      revisions.groupBy("head")
        .agg(
          count("*").as("revision_count") // Count the number of revisions for each page
        )
        .orderBy("head")
    }

    // Function: Changes per predicate
    def countChangesPerPredicate(data: Dataset[TemporalExtractionResult]): DataFrame = {
      data.groupBy("head", "rel")
        .agg(
          countDistinct("tail").as("unique_changes"),
          countDistinct("rFrom", "rUntil").as("all_changes")
        )
        .orderBy("head", "rel")
    }

    // Function: Snapshot creation for a given time window
    def createSnapshot(data: Dataset[TemporalExtractionResult], timestamp: Long, outputPath: Option[String] = None): Dataset[TemporalExtractionResult] = {

      // 1. Filter the data for the given time window
      val snapshot = data.filter(row => row.tFrom <= timestamp && timestamp < row.tUntil)

      // 2. Dynamically reorder columns based on CSVRow field order
//      val fieldOrder = classOf[TemporalExtractionResult].getDeclaredFields.map(_.getName)
//      val orderedSnapshot = snapshot.select(fieldOrder.map(col): _*)

      // 3. Write the snapshot to a CSV file with headers only if an outputPath is provided
//      outputPath.foreach { path =>
////        orderedSnapshot.write
//          .mode("overwrite")
////          .option("header", "true")
////          .csv(path)
//      }
//      orderedSnapshot
      snapshot
    }

  // Function: Count revisions over time
  def countStartRevisionsOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("start_time", from_unixtime($"tFrom" / 1000))
      .select($"start_time", $"rFrom")
      .distinct()
      .groupBy("start_time")
      .agg(countDistinct("rFrom").alias("count_start_revisions"))
      .orderBy("start_time")
  }
  // Function: Count revisions over time
  def countEndRevisionsOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tUntil" / 1000))
      .select($"end_time", $"rUntil")
      .distinct()
      .groupBy("end_time")
      .agg(countDistinct("rUntil").alias("count_end_revisions"))
      .orderBy("end_time")
  }

  def countStartTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("start_time", from_unixtime($"tFrom" / 1000))
      .select($"start_time", $"head", $"rel", $"tail")
      .distinct()
      .groupBy("start_time")
      .agg(count("*").alias("count_start_triples"))
      .orderBy("start_time")
  }
  def countEndTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tUntil" / 1000))
      .select($"end_time", $"head", $"rel", $"tail")
      .distinct()
      .groupBy("end_time")
      .agg(count("*").alias("count_end_triples"))
      .orderBy("end_time")
  }
  def countChangesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._

    val dataWithTimestamps = data
      .withColumn("start_time", from_unixtime($"tFrom" / 1000))
      .withColumn("end_time", from_unixtime($"tUntil" / 1000))

    // Union of start_time & end_time, to get all relevant timestamps
    val changes = dataWithTimestamps
      .select($"head", $"rel", $"tail", $"start_time".as("change_time"), lit(1).as("change_type"))
      .union(
        dataWithTimestamps
          .select($"head", $"rel", $"tail", $"end_time".as("change_time"),
            lit(-1).as("change_type")
          )
      )

    // count all changes
    changes
      .groupBy("change_time")
      .agg(count("*").alias("count_triple_changes"))
      .orderBy("change_time")
  }


}