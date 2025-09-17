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
  sql.setConf("spark.local.dir", "/local/d1/data/workspace/dbpedia-tkg/tmp")
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
//        .withColumn("tStart", $"tStart".cast("long"))
//        .withColumn("tEnd", $"tEnd".cast("long"))
//        .as[TemporalExtractionResult]

    val triples = df.distinct().count()
    val distinctTriples = df.select("head","rel","tail").distinct().count()
    val distinctEntities = df.select("head").distinct().count()
    val distinctTypes = df.filter(col("rel") === VOCAB.RDFType).select("tail").distinct().count()
    val distinctRelations = df.select(col("rel")).distinct().count()
    val distinctVersions = df.select("tStart").union(df.select("tEnd")).distinct().count()

    sql.sparkContext.parallelize(Seq(TKGSummary(triples,distinctTriples,distinctEntities, distinctTypes,distinctRelations, distinctVersions))).toDF()
  }



  // count distinct timestamps


  // TODO Marvin, Max
  def dailyWindowCounts(): Unit = {
    val df = sql.read.json(in)
      .withColumn("tStart", date_format(to_date(from_unixtime($"tStart".cast("long"))),"yyyy-MM-dd"))
      .withColumn("tEnd",date_format(to_date(from_unixtime($"tEnd".cast("long"))), "yyyy-MM-dd"))
      .select("tStart","tEnd").groupBy("tStart","tEnd").count().orderBy(desc("count")).show()

    //      .withColumn("tStart", $"tStart".cast("long"))00
    //      .withColumn("tEnd", $"tEnd".cast("long"))
    //      .as[TemporalExtractionResult]
    //      .withColumn("tStart",to_date($"tStart"))
    //      .withColumn("tEnd",to_date($"tEnd"))
  }

  def hourWindowDistribution(): DataFrame = {
    val df = sql.read.parquet(in)
//      .withColumn("tStart", date_format(to_date(from_unixtime($"tStart".cast("long"))),"yyyy-MM-dd hh"))
//      .withColumn("tEnd",date_format(to_date(from_unixtime($"tEnd".cast("long"))), "yyyy-MM-dd hh"))
      .withColumn("tStart", $"tStart".cast("long"))
      .withColumn("tEnd", $"tEnd".cast("long"))
      .filter($"tEnd" =!= Long.MaxValue)
      .select("tStart","tEnd") //.select("tStart","tEnd").count().orderBy(desc("count")).show()
      .withColumn("duration_hours", floor(($"tEnd" - $"tStart") / 3600))
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
          .withColumn("tStart", $"tStart".cast("long"))
          .withColumn("tEnd", $"tEnd".cast("long"))
          .as[TemporalExtractionResult]
        writeOut("countStartTriplesOverTime",EvalFunctions.countStartTriplesOverTime(df))
      }),
//      "countStartTriplesOverTime" -> (() => {
//        val df = sql.read.parquet(in.getPath)
//          .withColumn("tStart", $"tStart".cast("long"))
//          .withColumn("tEnd", $"tEnd".cast("long"))
//          .as[TemporalExtractionResult]
//        writeOut("countStartTriplesOverTime",EvalFunctions.countStartTriplesOverTime(df))
//      }),
      "countEndTriplesOverTime" -> (() => {
        val df = sql.read.parquet(in)
          .withColumn("tStart", $"tStart".cast("long"))
          .withColumn("tEnd", $"tEnd".cast("long"))
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
