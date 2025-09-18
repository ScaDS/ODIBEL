package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.VOCAB.{DBO, DBO_WPWL, RDFType, SKOS_BROADER, SKOS_CONCEPT, SKOS_PREFLABEL, SKOS_SUBJECT}
import ai.scads.odibel.datasets.wikitext.eval.metricsdata.DiffStatYearly
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import org.apache.spark.sql.functions.{col, count, from_unixtime, lit, when, year}
import org.apache.spark.sql.{DataFrame, Dataset}
import picocli.CommandLine.{Command, Option}

import java.io.File
import java.time.{LocalDate, ZoneId}
import java.util.concurrent.Callable
import scala.jdk.CollectionConverters._

@Command(name = "snapshot")
class SnapshotEval extends Callable[Int] {

  private val sql = SparkSessionUtil.sql

  import sql.implicits._

  @Option(names = Array("--in", "-i"), required = true)
  var in: String = _

  @Option(names = Array("--out", "-o"), required = true)
  var out: String = _

  @Option(names = Array("--functions", "-f"), split = ",", required = false)
  var functionNamesToExecute: java.util.ArrayList[String] = _

  def writeOut(name: String, ds: DataFrame): Unit = {
    ds.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(out)
  }

  def genWPLsubgraph(): Dataset[TemporalExtractionResult] = {
    val df = sql.read.parquet(in)
    df.withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]
      .filter(ter => ter.rel == DBO_WPWL)
  }

  def genDBOsubgraph(): Dataset[TemporalExtractionResult] = {
    val df = sql.read.parquet(in)
    df.withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]
      .filter(ter => (ter.rel == RDFType && ter.tail.startsWith(DBO)) || ter.rel.startsWith(DBO))
  }

  def genDBOsubgraphNoWPL(): Dataset[TemporalExtractionResult] = {
    val df = sql.read.parquet(in)
    df.withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]
      .filter(ter => ((ter.rel == RDFType && ter.tail.startsWith(DBO)) || ter.rel.startsWith(DBO) && ter.rel != VOCAB.DBO_WPWL))
  }

  def genCATsubgraph(): Dataset[TemporalExtractionResult] = {
    val df = sql.read.parquet(in)
    df.withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]
      .filter(ter => ter.tail == SKOS_CONCEPT + ">" || Set(SKOS_BROADER, SKOS_SUBJECT, SKOS_PREFLABEL).contains(ter.rel))
  }

  def getUnixTimestampFromDate(date: String, format: String = "yyyy-MM-dd"): Long = {
    val formatter = java.time.format.DateTimeFormatter.ofPattern(format)
    val localDate = LocalDate.parse(date, formatter)
    val zoneId = ZoneId.systemDefault()
    localDate.atStartOfDay(zoneId).toEpochSecond
  }

  def genSnapshot(unix_timestamp: Long, dataset: Dataset[TemporalExtractionResult]): Dataset[TemporalExtractionResult] = {
    dataset.filter({
      ter =>
        ter.tStart <= unix_timestamp && unix_timestamp <= ter.tEnd
    })
  }

  def genYearlySnapshots(start: Int, end: Int, monthDayPart: String = "-06-01"): Unit = {
    val df = sql.read.parquet(in)
      .withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .as[TemporalExtractionResult]

    (start to end) foreach {
      year =>
        val date = year + "-06-01"
        val snap = genSnapshot(getUnixTimestampFromDate(date), df)
        snap.write.parquet(out + "/" + date)
    }
  }

  final val keys = Seq("head", "rel", "tail")


  def yearlyTripleDiffStats(start: Int, end: Int, monthDayPart: String = "-06-01"): Dataset[DiffStatYearly] = {

    val stat = (start to end).sliding(2).map({
      case IndexedSeq(prev, curr) =>
        val previousDF = sql.read.parquet(s"${in}/$prev-06-01")
        val currentDf = sql.read.parquet(s"${in}/$curr-06-01")

        val diff = calculateDiffDF(currentDf, previousDF, keys)

        val previousSize = previousDF.count()
        val currentSize = currentDf.count()

        val numberOfAdds = diff.filter(col("diff") === 1).distinct().count()
        val numberOfDels = diff.filter(col("diff") === -1).distinct().count()

        DiffStatYearly(prev, curr, previousSize, currentSize, numberOfAdds, numberOfDels)
    })

    SparkSessionUtil.spark.sparkContext.parallelize(stat.toSeq).toDS()
  }

  def calculateDiffDF(currentDF: DataFrame, previousDF: DataFrame, joinColumns: Seq[String]): DataFrame = {
    /**
     * Calculate adds and deletes between two DataFrames and write results.
     *
     * Args:
     * currentDF (DataFrame): The current state DataFrame.
     * previousDF (DataFrame): The previous state DataFrame.
     * partitionKey (String): The column name used as the partition key.
     * outputPath (String): The output path for the result.
     */

    // Add a diff column directly to indicate adds (0) and deletes (1)
    val currentWithDiff = currentDF.withColumn("del", lit(1))
    val previousWithDiff = previousDF.withColumn("add", lit(-1))

    // Perform a full outer join on the partition key
    val joinedDF = currentWithDiff.join(
      previousWithDiff,
      joinColumns,
      "outer"
    )

    val diffDf = joinedDF.select(
      joinColumns.map(col) :+
        when(col("del").isNull, col("add")).otherwise(when(col("add").isNull, col("del"))).alias("diff"): _*
    ).filter(col("diff").isNotNull)

    diffDf
  }

  def outDegreeDistribution(data: DataFrame): DataFrame = {
    data.groupBy($"head")
      .agg(count($"tail").as("out_degree"))
      .groupBy("out_degree")
      .count()
  }

  def yearlyOutDegreeDistribution(start: Int, end: Int, monthDayPart: String = "-06-01"): DataFrame = {
    (start to end).map({
      year =>
        val date = s"$year-06-01"
        val df = sql.read.parquet(s"${in}/$date").as[TemporalExtractionResult]
          .select("head", "rel", "tail")
        outDegreeDistribution(df).withColumn("year",lit(year))
    }).reduce(_ union _)
  }

  def yearlyOutDegreeDistributionOnlyObjects(start: Int, end: Int, monthDayPart: String = "-06-01"): DataFrame = {
    (start to end).map({
      year =>
        val date = s"$year-06-01"
        val df = sql.read.parquet(s"${in}/$date").as[TemporalExtractionResult]
          .filter(! _.tail.startsWith("\""))
          .select("head", "rel", "tail")
        outDegreeDistribution(df).withColumn("year",lit(year))
    }).reduce(_ union _)
  }

  override def call(): Int = {
    val functions: Map[String, () => Unit] = Map(
      "genYearlySnapshots" -> (() => {
        genYearlySnapshots(2023, 2025)
      }),
      "yearlyTripleDiffStats" -> (() => {
        val df = yearlyTripleDiffStats(2023,2025)
        writeOut("yearlyTripleDiffStats", df.toDF())
      }),
      "genWPLsubgraph" -> (() => {
        genWPLsubgraph().write.parquet(out+"/WPL")
      }),
      "genDBOsubgraph" -> (() => {
        genDBOsubgraph().write.parquet(out+"/DBO")
      }),
      "genDBOsubgraphNoWPL" -> (() => {
        genDBOsubgraphNoWPL().write.parquet(out+"/DBOnoWPL")
      }),
      "genCATsubgraph" -> (() => {
        genCATsubgraph().write.parquet(out+"/CAT")
      }),
      "yearlyOutDegreeDistribution" -> (() => {
        writeOut("yearlyOutDegreeDistribution",yearlyOutDegreeDistribution(2000,2025))
      }),
      "yearlyOutDegreeDistributionOnlyObjects" -> (() => {
        writeOut("yearlyOutDegreeDistributionOnlyObjects",yearlyOutDegreeDistributionOnlyObjects(2000,2025))
      })
    )

    if (functionNamesToExecute == null)
      println(functions.keys.mkString(","))
    else {
      functionNamesToExecute.asScala.foreach({
        functionName =>
          functions.get(functionName).foreach(_())
      })
    }
    0
  }
}
