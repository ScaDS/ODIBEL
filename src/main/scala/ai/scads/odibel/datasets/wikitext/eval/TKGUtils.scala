package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.eval.CSVRow
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


object TKGUtils {

  // Function: Calculate all unique windows
  def calculateAllUniqueWindows(data: Dataset[CSVRow]): Long = {
    import data.sparkSession.implicits._
    data.map(row => row.tFrom.toString + row.tUntil.toString: String).distinct().count()
  }

  // Function: Count triples per subject
  def countTriplesPerSubject(data: Dataset[CSVRow]): DataFrame = {
    data.select("head", "rel", "tail") // Select only the relevant fields
      .distinct() // Remove duplicates
      .groupBy("head") // Group by subject (head)
      .agg(
        count("*").as("triple_count") // Count unique triples per subject
      )
  }

  // Function: Revisions per page
  def calculateRevisionsPerPage(data: Dataset[CSVRow]): DataFrame = {
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
  }

  // Function: Changes per predicate
  def calculateChangesPerPredicate(data: Dataset[CSVRow]): DataFrame = {
    data.groupBy("head", "rel")
      .agg(
        countDistinct("tail").as("unique_changes"),
        countDistinct("rFrom", "rUntil").as("all_changes")
      )
  }

  // Function: Snapshot creation for a given time window
  def createSnapshot(data: Dataset[CSVRow], timestamp: Long, outputPath: Option[String] = None): DataFrame = {

    // 1. Filter the data for the given time window
    val snapshot = data.filter(row => row.tFrom <= timestamp && timestamp <= row.tUntil)

    // 2. Dynamically reorder columns based on CSVRow field order
    val fieldOrder = classOf[CSVRow].getDeclaredFields.map(_.getName)
    val orderedSnapshot = snapshot.select(fieldOrder.map(col): _*)

    // 3. Write the snapshot to a CSV file with headers only if an outputPath is provided
    outputPath.foreach { path =>
      orderedSnapshot.write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
    orderedSnapshot
  }

}