package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.metricsdata.{ElementDate, ElementWindow}
import ai.scads.odibel.datasets.wikitext.utils.CronUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions => F}
import org.apache.spark.sql.functions._

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId}

@Deprecated
object EvalFunctions {

  //   Function: Calculate all unique windows
  def countAllUniqueWindows(data: Dataset[TemporalExtractionResult]): Long = {
    import data.sparkSession.implicits._
    data.map(row => row.tStart.toString + row.tEnd.toString: String).distinct().count()
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
        .select(data("head"), data("rStart"), data("rEnd")) // Select relevant columns

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
          countDistinct("rStart", "rEnd").as("all_changes")
        )
        .orderBy("head", "rel")
    }

    // Function: Snapshot creation for a given time window
    def createSnapshot(data: Dataset[TemporalExtractionResult], timestamp: Long, outputPath: Option[String] = None): Dataset[TemporalExtractionResult] = {

      // 1. Filter the data for the given time window
      val snapshot = data.filter(row => row.tStart <= timestamp && timestamp < row.tEnd)

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
    data.withColumn("start_time", from_unixtime($"tStart"))
      .select($"start_time", $"rStart")
      .groupBy("start_time")
      .agg(countDistinct("rStart").alias("count_start_revisions"))
      .orderBy("start_time")
  }
  // Function: Count ending revisions over time
  def countEndRevisionsOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tEnd"))
      .select($"end_time", $"rEnd")
      .groupBy("end_time")
      .agg(countDistinct("rEnd").alias("count_end_revisions"))
      .orderBy("end_time")
  }

  // Function: Count starting triples over time
  def countStartTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("start_time", from_unixtime($"tStart"))
      .select($"year", $"head", $"rel", $"tail")
      .distinct()
      .withColumn("year", year($"start_time"))
      .groupBy("year")
      .agg(count("*").alias("count_start_triples"))
      .orderBy("year")
  }
  // Function: Count ending triples over time
  def countEndTriplesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._
    data.withColumn("end_time", from_unixtime($"tEnd"))
      .select($"year", $"head", $"rel", $"tail")
      .distinct()
      .withColumn("year", year($"end_time"))
      .groupBy("year")
      .agg(count("*").alias("count_end_triples"))
      .orderBy("year")
  }

  // Function: Count changes of triples over time (new and deleted triples)
  def countChangesOverTime(data: Dataset[TemporalExtractionResult]): DataFrame = {
    import data.sparkSession.implicits._

    val dataWithTimestamps = data
      .withColumn("start_time", from_unixtime($"tStart"))
      .withColumn("end_time", from_unixtime($"tEnd"))

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


  def filterForResources(data: Dataset[TemporalExtractionResult], filterBySubject: Boolean) = {
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
    filteredResources
  }

  // Function: Calculate In-Degree Frequency Distribution per Year
  def calculateInDegreeFrequency(data: Dataset[TemporalExtractionResult], filterBySubject: Boolean = false, filterOutliers: Boolean = false) = {
    import data.sparkSession.implicits._
    val filteredResources = this.filterForResources(data = data, filterBySubject = filterBySubject)
    filteredResources
      .withColumn("year", year(from_unixtime($"tStart")))
      .groupBy($"year", $"tail")
      .agg(count($"head").as("in_degree"))
      .groupBy($"in_degree", $"year")
      .agg(count("*").as("frequency"))
      .orderBy("in_degree", "year")
  }

  // Function: Calculate In-Degree Distribution per Year
  def calculateInDegreeDistributionPerYear(data: Dataset[TemporalExtractionResult], filterBySubject: Boolean = false) = {
    import data.sparkSession.implicits._
    val filteredResources = this.filterForResources(data = data, filterBySubject = filterBySubject)
    filteredResources
      .withColumn("year", year(from_unixtime($"tStart")))
      .groupBy($"year", $"tail")
      .agg(count($"head").as("in_degree"))
      .groupBy($"year")
      .agg(
        avg("in_degree").as("mean"),
        expr("percentile_approx(in_degree, 0.5)").as("median"),
        stddev("in_degree").as("std"),
        min("in_degree").as("min"),
        max("in_degree").as("max"),
        F.expr("percentile_approx(in_degree, 0.25)").as("q25"),
        F.expr("percentile_approx(in_degree, 0.75)").as("q75")
      )
      .orderBy("year")
  }

  // Function: Calculate Out-Degree Frequency per Year
  def calculateOutDegreeFrequency(data: Dataset[TemporalExtractionResult]) = {
    import data.sparkSession.implicits._
    data
      .withColumn("year", year(from_unixtime($"tStart")))
      .groupBy($"year", $"head")
      .agg(count($"tail").as("out_degree"))
      .groupBy($"out_degree", $"year")
      .agg(count("*").as("frequency"))
      .orderBy("out_degree", "year")
  }

  // Function: Calculate Out-Degree Distribution per Year
  def calculateOutDegreeDistributionPerYear(data: Dataset[TemporalExtractionResult]) = {
    import data.sparkSession.implicits._
    data
      .withColumn("year", year(from_unixtime($"tStart")))
      .groupBy($"year", $"head")
      .agg(count($"tail").as("out_degree"))
      .groupBy($"year")
      .agg(
        avg("out_degree").as("mean"),
        expr("percentile_approx(out_degree, 0.5)").as("median"),
        stddev("out_degree").as("std"),
        min("out_degree").as("min"),
        max("out_degree").as("max"),
        F.expr("percentile_approx(out_degree, 0.25)").as("q25"),
        F.expr("percentile_approx(out_degree, 0.75)").as("q75")
      )
      .orderBy("year")
  }


  // Function: Calculate Temporal Activity Span
  // Computes median, mean, std, min, max, and quantiles of the time span (tStart to tEnd) for RDF triples
  def calculateTemporalActivitySpanOverTime(data: Dataset[TemporalExtractionResult]) = {
    import data.sparkSession.implicits._

    // Filter out invalid entries where tStart > tEnd
    val validData = data.filter(F.col("tStart") <= F.col("tEnd"))

    // Add a year column based on tStart
    val dataWithYear = validData.withColumn("year", F.year(F.from_unixtime(F.col("tStart"))))

    // Calculate the duration for each triple (in milliseconds)
    val dataWithDuration = dataWithYear.withColumn("duration", F.col("tEnd") - F.col("tStart"))

    // Group by year and calculate statistics for the duration
    val stats = dataWithDuration
      .groupBy("year")
      .agg(
        F.avg("duration").as("mean"),
        F.expr("percentile_approx(duration, 0.5)").as("median"),
        F.stddev("duration").as("std"),
        F.min("duration").as("min"),
        F.max("duration").as("max"),
        F.expr("percentile_approx(duration, 0.25)").as("q25"),
        F.expr("percentile_approx(duration, 0.75)").as("q75")
      )
      .orderBy("year")

    // Return the statistics
    stats
  }

  // Function: Calculate Temporal Activity Span
  // Computes median, mean, std, min, max, and quantiles of the time span (tStart to tEnd) for RDF triples
  def calculateTemporalActivitySpan(data: Dataset[TemporalExtractionResult]) = {
    import data.sparkSession.implicits._

    // Filter out invalid entries where tStart > tEnd
    val validData = data.filter(F.col("tStart") <= F.col("tEnd"))

    // Calculate the duration for each triple (in milliseconds)
    val dataWithDuration = validData.withColumn("duration", F.col("tEnd") - F.col("tStart"))

    // Group by year and calculate statistics for the duration
    val stats = dataWithDuration
      .agg(
        avg("duration").as("mean"),
        expr("percentile_approx(duration, 0.5)").as("median"),
        stddev("duration").as("std"),
        min("duration").as("min"),
        max("duration").as("max"),
        F.expr("percentile_approx(duration, 0.25)").as("q25"),
        F.expr("percentile_approx(duration, 0.75)").as("q75")
      )

    // Return the statistics
    stats
  }

  // Function: Calculate Subject Over Time
  def countPartsOfTriplesOverTime(data: Dataset[TemporalExtractionResult], count_triple_part: String = "subject", time_resolution: String = "yearly") = {
    import data.sparkSession.implicits._

    // Zeitauflösung für Starts: Basierend auf tStart
    val startsWithTime = time_resolution match {
      case "monthly" => data.withColumn("time", F.date_format(F.from_unixtime(F.col("tStart") ), "yyyy-MM"))
      case _         => data.withColumn("time", F.year(F.from_unixtime(F.col("tStart"))))
    }

    // Zeitauflösung für Ends: Basierend auf tEnd
    val endsWithTime = time_resolution match {
      case "monthly" => data.withColumn("time", F.date_format(F.from_unixtime(F.col("tEnd")), "yyyy-MM"))
      case _         => data.withColumn("time", F.year(F.from_unixtime(F.col("tEnd"))))
    }

    val triple_part = count_triple_part.toLowerCase() match {
      case "subject"    => "head"
      case "predicate"  => "rel"
      case "object"     => "tail"
      case _            => count_triple_part.toLowerCase()
    }

    val starts = startsWithTime
      .select(F.col("time"), F.col(triple_part))
      .distinct()
      .groupBy("time")
      .agg(F.count(triple_part).alias(s"new_$triple_part"))

    val ends = endsWithTime
      .select(F.col("time"), F.col(triple_part))
      .distinct()
      .groupBy("time")
      .agg(F.count(triple_part).alias(s"ended_$triple_part"))

    val valid = data
      .withColumn("time", time_resolution match {
        case "monthly" => F.date_format(F.from_unixtime(F.col("tStart") / 1000), "yyyy-MM")
        case _          => F.year(F.from_unixtime(F.col("tStart") / 1000))
      })
      .select(F.col("time"), F.col(triple_part))
      .distinct()
      .groupBy("time")
      .agg(F.count(triple_part).alias(s"valid_$triple_part"))

    val combined = starts
      .join(ends, Seq("time"), "outer")
      .join(valid, Seq("time"), "outer")
      .withColumn(s"new_$triple_part", F.coalesce(F.col(s"new_$triple_part"), F.lit(0)))
      .withColumn(s"ended_$triple_part", F.coalesce(F.col(s"ended_$triple_part"), F.lit(0)))
      .withColumn(s"valid_$triple_part", F.coalesce(F.col(s"valid_$triple_part"), F.lit(0)))
      .withColumn("changes", F.col(s"new_$triple_part") + F.col(s"ended_$triple_part"))
      .orderBy("time")

    combined
  }

  // Function: Generate Dataset Statistics like the Benchmark Image
    def calculateSnapshotStatistics(data: Dataset[TemporalExtractionResult], granularities: Seq[String] = Seq("yearly", "monthly", "instant")) = {
    import data.sparkSession.implicits._

    // Helper function to group data by a given granularity
    def groupByGranularity(data: Dataset[TemporalExtractionResult], granularity: String) = {
      granularity match {
        case "instant" => data.withColumn("time", F.lit("instant"))
        case "hourly" => data.withColumn("time", F.date_format(F.from_unixtime(F.col("tStart")), "yyyy-MM-dd HH"))
        case "daily" => data.withColumn("time", F.date_format(F.from_unixtime(F.col("tStart")), "yyyy-MM-dd"))
        case "monthly" => data.withColumn("time", F.date_format(F.from_unixtime(F.col("tStart")), "yyyy-MM"))
        case "yearly" => data.withColumn("time", F.year(F.from_unixtime(F.col("tStart"))))
        case _ => data.withColumn("time", F.lit("unknown"))
      }
    }

    // Aggregate statistics for each granularity
    val statsByGranularity = granularities.map { granularity =>
      val groupedData = groupByGranularity(data, granularity)

      // Calculate total triples in the first and last version using tStart and tEnd
      val dataWithDate = data.withColumn("date", F.from_unixtime(F.col("tStart")).cast("date"))
        .withColumn("year", F.year(F.col("date")))
      val filteredData = dataWithDate.filter(F.col("year") > 1970)
      val sortedData = filteredData.orderBy(F.asc("tStart"))
      val secondMin = sortedData.limit(2).collect() // Collect the first two rows
      val min_tStart = if (secondMin.length == 2) secondMin(1).getAs[Long]("tStart") else null
      val max_tStart = data.agg(F.max("tStart")).as[Long].first()

      val triplesInFirstVersion = data.filter(F.col("tStart") === min_tStart).count()
      val triplesInLastVersion = data.filter(F.col("tStart") === max_tStart).count()

      // Calculate growth percentage
      val growth = (triplesInLastVersion.toDouble / triplesInFirstVersion.toDouble) * 100

      // Calculate change ratios
      val totalTriples = data.count().toDouble
      val changeRatioAdds = (data.filter(F.col("tStart") > min_tStart).count().toDouble / totalTriples) * 100
      val changeRatioDeletes = (data.filter(F.col("tEnd") < max_tStart).count().toDouble / totalTriples) * 100

      // Static core: Triples that exist throughout all versions
      val staticCore = data.filter(F.col("tStart") === min_tStart && F.col("tEnd") === max_tStart).count()

      // Version-oblivious triples: All unique triples regardless of their time span
      val versionObliviousTriples = data.filter(F.col("tStart") =!= min_tStart || F.col("tEnd") =!= max_tStart).count()

      // Count the number of versions for the current granularity
      val versions = groupedData.select("time").distinct().count()

      (granularity, versions, triplesInFirstVersion, triplesInLastVersion, growth, changeRatioAdds, changeRatioDeletes, staticCore, versionObliviousTriples)
    }

    // Convert the results into a DataFrame
    val statsDF = statsByGranularity.toDF("Granularity", "Versions", "Triples in First Version", "Triples in Last Version", "Growth", "Change ratio adds", "Change ratio deletes", "Static core", "Version-oblivious triples")

    statsDF
  }

  def datesByColumn(df: DataFrame, column: Column): Dataset[ElementDate] = {
    import df.sparkSession.implicits._
    df.withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]
      .select(column.as("element"),$"tStart",$"tEnd")
      .as[ElementWindow]
      .map(ew => if(ew.tEnd == Long.MaxValue) ew.copy(tEnd = 1767351600) else ew)
      .flatMap(ew => {
        val dates = CronUtil.findCronOccurrencesBetween(ew.tStart, ew.tEnd)
        dates.map(date => ElementDate(ew.element,date.getYear.toString))
      })
  }

  def intervalToYearMonthDay(startUnix: Long, endUnix: Long, zoneId: ZoneId = ZoneId.systemDefault()): Seq[String] = {
    // Convert UNIX timestamps (in seconds) to LocalDates
    val startDate = Instant.ofEpochSecond(startUnix).atZone(zoneId).toLocalDate
    val endDate   = Instant.ofEpochSecond(endUnix).atZone(zoneId).toLocalDate

    // Calculate the number of days between the start and end dates, inclusive
    val daysBetween = ChronoUnit.YEARS.between(startDate, endDate).toInt

    // Generate a sequence of (year, month, day) tuples
    (0 to daysBetween).map { i =>
      val currentDay = startDate.plusYears(i)
      //      (currentDay.getYear.toString, 1) // currentDay.getMonthValue, currentDay.getDayOfMonth)
      currentDay.getYear.toString
    }
  }

}