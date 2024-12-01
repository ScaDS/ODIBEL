package ai.scads.odibel.datasets.wikitext.eval


import ai.scads.odibel.datasets.wikitext.TemporalExtractionResult
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{avg, max, min}
import org.apache.spark.sql.types.{LongType, StructType}
import picocli.CommandLine.{Command, Option}

import java.io.File
import java.util.concurrent.Callable

@Command(name = "eval", mixinStandardHelpOptions = true)
class TKGEval extends Callable[Int] {

  @Option(names = Array("--in", "-i"), required = true)
  var in: File = _

  @Option(names = Array("--out", "-o"), required = true)
  var out: File = _

  @Option(names = Array("--functions", "-f"), required = false)
  var func: Array[String] = _ // TODO filter by functions

  def writeOut(name: String, ds: DataFrame): Unit = {
    ds.write.mode("overwrite").option("header", "true")
              .csv(new File(out, name).getPath)
  }

  override def call(): Int = {
    // Create Spark Session
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sql = spark.sqlContext

    import sql.implicits._

    val data = sql.read.json(in.getPath)
      .withColumn("tFrom", $"tFrom".cast("long"))
      .withColumn("tUntil", $"tUntil".cast("long"))
      .as[TemporalExtractionResult]

    val uniqueWindows = EvalFunctions.countAllUniqueWindows(data)
    import scala.jdk.CollectionConverters._
    writeOut("uniqueWindows", Seq(uniqueWindows).toDF("uniqueWindows"))

    val triplesPerSubject = EvalFunctions.countTriplesPerSubject(data)
      .agg(
        min("triple_count").alias("min"),
        max("triple_count").alias("max"),
        avg("triple_count").alias("avg")
      )
    writeOut("triplesPerSubject", triplesPerSubject)

    val revisionsPerPage = EvalFunctions.countRevisionsPerPage(data)
    writeOut("revisionsGroupedByPage",revisionsPerPage)

    val changesPerPredicate = EvalFunctions.countChangesPerPredicate(data)
    writeOut("changesByPredicate", changesPerPredicate)

    spark.stop()
    0
  }
}
