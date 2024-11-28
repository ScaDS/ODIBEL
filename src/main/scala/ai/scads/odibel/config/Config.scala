package ai.scads.odibel.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkC, SparkContext}

object Config {

  val config: Config = ConfigFactory.load("application.conf")
  val test: Config = config.getConfig("test")
  val test_filepath: String = test.getValue("filepath").unwrapped().asInstanceOf[String]

  def main(args: Array[String]): Unit  =  {
    println(test_filepath)
  }

  object Spark {

    def getConfig: SparkConf = {

      new SparkConf().setMaster("local[*]")
    }
  }
}
