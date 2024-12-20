package ai.scads.odibel.datasets.wikitext.eval

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

object EvalSpark {

  val sparkConf = new SparkConf
  sys.env.get("SPARK_MASTER").map({
    SPARK_MASTER =>
      sparkConf.setMaster(SPARK_MASTER)
  })

  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val sql: SQLContext = spark.sqlContext

}
