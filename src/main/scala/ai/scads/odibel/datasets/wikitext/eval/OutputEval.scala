package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.metricsdata.TKGSummary
import ai.scads.odibel.datasets.wikitext.eval.{EvalFunctions, VOCAB}
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, datediff, desc, floor, from_unixtime, to_date}
import picocli.CommandLine.{Command, Option}

import scala.jdk.CollectionConverters._
import java.io.File
import java.util.concurrent.Callable

@Command(name = "output")
class OutputEval extends Callable[Int] {

  // Property Changes filter Max Long, group by property count
  // Type Changes (requires join)

  @Option(names = Array("--in", "-i"), required = true)
  var in: String = _

  @Option(names = Array("--out", "-o"), required = true)
  var out: String = _

  @Option(names = Array("--functions", "-f"), split = ",", required = false)
  var functionNamesToExecute: java.util.ArrayList[String] = _

  private val sql = SparkSessionUtil.sql
  import sql.implicits._

  def writeOut(name: String, ds: DataFrame): Unit = {
    ds.repartition(100)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(out)
  }
  // start end triple stats

  // TODO Max
  // calculateTemporalActivitySpanOverTime

  // count triples
  // count entities (union sub, obj)
  // count types (classes used)
  // count relations (predicate vocabulary)
  def summary(): DataFrame = {
    val df = sql.read.parquet(in)
//        .withColumn("tFrom", $"tFrom".cast("long"))
//        .withColumn("tUntil", $"tUntil".cast("long"))
//        .as[TemporalExtractionResult]

    val triples = df.distinct().count()
    val distinctTriples = df.select("head","rel","tail").distinct().count()
    val distinctEntities = df.select("head").distinct().count()
    val distinctTypes = df.filter(col("rel") === VOCAB.RDFType).select("tail").distinct().count()
    val distinctRelations = df.select(col("rel")).distinct().count()
    val distinctVersions = df.select("tFrom").union(df.select("tUntil")).distinct().count()

    sql.sparkContext.parallelize(Seq(TKGSummary(triples,distinctTriples,distinctEntities, distinctTypes,distinctRelations, distinctVersions))).toDF()
  }



  // count distinct timestamps


  // TODO Marvin, Max
  def dailyWindowCounts(): Unit = {
    val df = sql.read.json(in)
      .withColumn("tFrom", date_format(to_date(from_unixtime($"tFrom".cast("long"))),"yyyy-MM-dd"))
      .withColumn("tUntil",date_format(to_date(from_unixtime($"tUntil".cast("long"))), "yyyy-MM-dd"))
      .select("tFrom","tUntil").groupBy("tFrom","tUntil").count().orderBy(desc("count")).show()

    //      .withColumn("tFrom", $"tFrom".cast("long"))00
    //      .withColumn("tUntil", $"tUntil".cast("long"))
    //      .as[TemporalExtractionResult]
    //      .withColumn("tFrom",to_date($"tFrom"))
    //      .withColumn("tUntil",to_date($"tUntil"))
  }

  def hourWindowDistribution(): DataFrame = {
    val df = sql.read.parquet(in)
//      .withColumn("tFrom", date_format(to_date(from_unixtime($"tFrom".cast("long"))),"yyyy-MM-dd hh"))
//      .withColumn("tUntil",date_format(to_date(from_unixtime($"tUntil".cast("long"))), "yyyy-MM-dd hh"))
      .withColumn("tFrom", $"tFrom".cast("long"))
      .withColumn("tUntil", $"tUntil".cast("long"))
      .filter($"tUntil" =!= Long.MaxValue)
      .select("tFrom","tUntil") //.select("tFrom","tUntil").count().orderBy(desc("count")).show()
      .withColumn("duration_hours", floor(($"tUntil" - $"tFrom") / 3600))
      .filter($"duration_hours" >= 0)
      .groupBy("duration_hours").count()
    df.orderBy("duration_hours")
  }

  override def call(): Int = {
    val functions: Map[String, () => Unit] = Map(
      "summary" -> (() => {
        writeOut("summary",summary())
      }),
      "hourWindowDistribution"-> (() => {
        writeOut("hourWindowDistribution",hourWindowDistribution())
      }),
      "countStartTriplesOverTime" -> (() => {
        val df = sql.read.parquet(in)
          .withColumn("tFrom", $"tFrom".cast("long"))
          .withColumn("tUntil", $"tUntil".cast("long"))
          .as[TemporalExtractionResult]
        writeOut("countStartTriplesOverTime",EvalFunctions.countStartTriplesOverTime(df))
      }),
//      "countStartTriplesOverTime" -> (() => {
//        val df = sql.read.parquet(in.getPath)
//          .withColumn("tFrom", $"tFrom".cast("long"))
//          .withColumn("tUntil", $"tUntil".cast("long"))
//          .as[TemporalExtractionResult]
//        writeOut("countStartTriplesOverTime",EvalFunctions.countStartTriplesOverTime(df))
//      }),
      "countEndTriplesOverTime" -> (() => {
        val df = sql.read.parquet(in)
          .withColumn("tFrom", $"tFrom".cast("long"))
          .withColumn("tUntil", $"tUntil".cast("long"))
          .as[TemporalExtractionResult]
        writeOut("countEndTriplesOverTime",EvalFunctions.countEndTriplesOverTime(df))
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
