package ai.scads.odibel.datasets.wikitext.eval

.sql

import ai.scads.odibel.datasets.wikitext.eval.rows.TKGSummary
import ai.scads.odibel.datasets.wikitext.eval.{EvalSpark, VOCAB}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, desc, from_unixtime, to_date}
import picocli.CommandLine.{Command, Option}

import java.io.File
import java.util.concurrent.Callable

@Command(name = "output")
class OutputEval extends Callable[Int] {

  // Property Changes filter Max Long, group by property count
  // Type Changes (requires join)

  @Option(names = Array("--in", "-i"), required = true)
  var in: File = _

  @Option(names = Array("--out", "-o"), required = true)
  var out: File = _

  @Option(names = Array("--functions", "-f"), split = ",", required = false)
  var functionNamesToExecute: java.util.ArrayList[String] = _

  private val sql = EvalSpark.sql
  import sql.implicits._

  def writeOut(name: String, ds: DataFrame): Unit = {
    ds.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(new File(out, name).getPath)
  }
  // start end triple stats

  // TODO Max
  // calculateTemporalActivitySpanOverTime

  // count triples
  // count entities (union sub, obj)
  // count types (classes used)
  // count relations (predicate vocabulary)
  def tgkSummary(): DataFrame = {
    val df = sql.read.json(in.getPath)

    val triples = df.distinct().count()
    val distinctTriples = df.select("head","rel","tail").distinct().count()
    val distinctEntities = df.select("head").distinct().count()
    val distinctTypes = df.filter(col("rel") === VOCAB.RDFType).select("tail").distinct().count()
    val distinctRelations = df.select(col("rel")).distinct().count()
    val distinctVersions = df.select("tUntil").distinct().count()

    sql.sparkContext.parallelize(Seq(TKGSummary(triples,distinctTriples,distinctEntities, distinctTypes,distinctRelations, distinctVersions))).toDF()
  }



  // count distinct timestamps


  // TODO Marvin, Max
  def dailyWindowCounts(): Unit = {
    val df = sql.read.json(in.getPath)
      .withColumn("tFrom", date_format(to_date(from_unixtime($"tFrom".cast("long"))),"yyyy-MM-dd"))
      .withColumn("tUntil",date_format(to_date(from_unixtime($"tUntil".cast("long"))), "yyyy-MM-dd"))
      .select("tFrom","tUntil").groupBy("tFrom","tUntil").count().orderBy(desc("count")).show()

    //      .withColumn("tFrom", $"tFrom".cast("long"))00
    //      .withColumn("tUntil", $"tUntil".cast("long"))
    //      .as[TemporalExtractionResult]
    //      .withColumn("tFrom",to_date($"tFrom"))
    //      .withColumn("tUntil",to_date($"tUntil"))
  }

  override def call(): Int = {
//    tgkSummary().show()
    0
  }
}
