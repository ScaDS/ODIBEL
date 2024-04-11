package ai.scads.odibel

import org.apache.spark.sql.{SQLContext, SparkSession}
import picocli.CommandLine

object Main extends App {
//  System.setProperty("scala.time","true")

  // val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  // sparkSession.sparkContext.setLogLevel("INFO")
//  implicit val sqlContext: SQLContext = sparkSession.sqlContext

  new CommandLine(new Odibel).execute(args: _*)
}

