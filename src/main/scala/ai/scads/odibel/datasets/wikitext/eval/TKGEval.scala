package ai.scads.odibel.datasets.wikitext.eval

import org.apache.spark.sql.SparkSession

object TKGEval extends App {

  val in = args(0)

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  val sql = spark.sqlContext
  import sql.implicits._

  sql.read.json(in).select("rel").groupBy("rel").count().show(false)

}
