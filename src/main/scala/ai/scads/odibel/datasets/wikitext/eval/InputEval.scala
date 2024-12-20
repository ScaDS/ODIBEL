package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.eval.rows.RevisionMetadata
import ai.scads.odibel.datasets.wikitext.{FlatRawPageRevision, WikiUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import picocli.CommandLine.{Command, Option}

import scala.jdk.CollectionConverters._
import java.io.File
import java.util.concurrent.Callable

@Command(name = "input")
class InputEval extends Callable[Int] {

  // Input Stats
  // - [x] generate small input dumps MetaEntry(pid,rid,t,ns)
  // - [x] total pages, article pages, category pages
  // - [x] total revisions, article revisions, category revisions

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

  def extractPageMetaTable(): Unit = {
    val df = sql.read.json(in.getPath)
    val mapped = df.as[FlatRawPageRevision].map({
      raw =>
        val enriched = WikiUtil.enrichFlatRawPageRevision(raw)
        RevisionMetadata(enriched.pId, enriched.rId, enriched.rTimestamp, enriched.ns.get)
    })
    mapped.repartition(64).write.mode("overwrite").parquet(out.getPath)
  }

  final val timestampFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"


  def namespaceRevisionCount(): DataFrame = {
    val df = sql.read.parquet(in.getPath).select(col("id").as("rid"),col("pageid").as("pid"),col("timestamp"),col("namespace").as("ns"))
    df.select("rid", "ns").distinct().select("ns").groupBy("ns").count()
  }

  def namespacePageCount(): DataFrame = {
    val df = sql.read.parquet(in.getPath).select(col("id").as("rid"),col("pageid").as("pid"),col("timestamp"),col("namespace").as("ns"))
    df.select("pid", "ns").distinct().select("ns").groupBy("ns").count()
  }

  def revisionsPerYear(): DataFrame = {
    val df = sql.read.parquet(in.getPath).select(col("id").as("rid"),col("pageid").as("pid"),col("timestamp"),col("namespace").as("ns"))
      .withColumn("year", date_format(to_date(to_timestamp($"timestamp"),timestampFormat), "yyyy"))
    df.select("year","ns").groupBy("year","ns").count()
  }

  def pagesPerYear(): DataFrame = {
    val df = sql.read.parquet(in.getPath).select(col("id").as("rid"),col("pageid").as("pid"),col("timestamp"),col("namespace").as("ns"))
      .withColumn("year", date_format(to_date(to_timestamp($"timestamp"),timestampFormat), "yyyy"))
    df.select("pid", "year", "ns").groupBy("pid", "ns").agg(min("year").alias("min_year")).select(col("min_year").as("year"),col("ns")).groupBy("year", "ns").count()

  }

  override def call(): Int = {

    val functions: Map[String, () => Unit] = Map(
      "extractPageMetaTable" -> (() => {
        extractPageMetaTable()
      }),
      "namespaceRevisionCount" -> (() => {
        writeOut("namespaceRevisionCount", namespaceRevisionCount())
      }),
      "revisionsPerYear" -> (() => {
        writeOut("revisionsPerYear",revisionsPerYear())
      }),
      "pagesPerYear" -> (() => {
        writeOut("pagesPerYear", pagesPerYear())
      }),
      "namespacePageCount" -> (() => {
        writeOut("namespacePageCount",namespacePageCount())
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
