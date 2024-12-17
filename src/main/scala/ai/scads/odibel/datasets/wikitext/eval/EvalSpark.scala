package ai.scads.odibel.datasets.wikitext.eval

import org.apache.spark.sql.{SQLContext, SparkSession}

object EvalSpark {

  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val sql: SQLContext = spark.sqlContext

}
