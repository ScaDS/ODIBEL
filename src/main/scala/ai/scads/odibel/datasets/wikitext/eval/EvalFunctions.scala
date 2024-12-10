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

  // Function: Count starting revisions over time
  def countStartRevisionsOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("start_time", from_unixtime($"tFrom" / 1000))
      .select($"start_time", $"rFrom")
      .groupBy("start_time")
      .agg(countDistinct("rFrom").alias("count_start_revisions"))
      .orderBy("start_time")
  }
  // Function: Count ending revisions over time
  def countEndRevisionsOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tUntil" / 1000))
      .select($"end_time", $"rUntil")
      .groupBy("end_time")
      .agg(countDistinct("rUntil").alias("count_end_revisions"))
      .orderBy("end_time")
  }
  // Function: Count starting triples over time
  def countStartTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("start_time", from_unixtime($"tFrom" / 1000))
      .select($"start_time", $"head", $"rel", $"tail")
      .distinct()
      .groupBy("start_time")
      .agg(count("*").alias("count_start_triples"))
      .orderBy("start_time")
  }
  // Function: Count ending triples over time
  def countEndTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tUntil" / 1000))
      .select($"end_time", $"head", $"rel", $"tail")
      .distinct()
      .groupBy("end_time")
      .agg(count("*").alias("count_end_triples"))
      .orderBy("end_time")
  }
  // Function: Count changes of triples over time (new and deleted triples)
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

  // Function: Calculate In-Degree Distribution per Year
  def calculateInDegreeDistributionPerYear(data: Dataset[TemporalExtractionResult], filterBySubject: Boolean = false) = {
    import data.sparkSession.implicits._
    // filter to ensure only URIs are considered as 'tail'
    val uris = data.filter($"tail".startsWith("http://"))

    // Optionally filter to ensure only resources that also appear as 'head' are considered
    val filteredResources = if (filterBySubject) {
      val subjects = data.select("head").distinct()
      uris.join(subjects, uris("tail") === subjects("head"))
    } else {
      uris
    }

    // calculate In-Degree
    filteredResources
      .withColumn("year", year(from_unixtime($"tFrom" / 1000)))
      .groupBy($"year", $"tail")
      .agg(count($"head").as("in_degree")) // Count the number of incoming edges for each tail
      .orderBy("year", "tail")
  }

  // Function: Calculate Out-Degree Distribution per Year
  def calculateOutDegreeDistributionPerYear(data: Dataset[TemporalExtractionResult]) = {
    import data.sparkSession.implicits._
    data
      .withColumn("year", year(from_unixtime($"tFrom" / 1000)))
      .groupBy($"year", $"head")
      .agg(count($"tail").as("out_degree")) // Count the number of outgoing edges for each head
      .orderBy("year", "head")
  }


}