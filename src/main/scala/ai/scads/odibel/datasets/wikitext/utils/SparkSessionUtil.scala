package ai.scads.odibel.datasets.wikitext.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Spark Session utility for standalone and cluster usage
 */
object SparkSessionUtil {

  val sparkConf = new SparkConf
  sys.env.get("SPARK_MASTER") match {
    case Some(value) =>
      sparkConf.setMaster(value)
    case None =>
  }

  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val sql: SQLContext = spark.sqlContext

}
