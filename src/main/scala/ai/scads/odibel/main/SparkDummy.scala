package ai.scads.odibel.main

import org.apache.spark.sql.SparkSession
import picocli.CommandLine.Command

import java.util.concurrent.Callable

@Command(name = "spark-dummy", mixinStandardHelpOptions = true)
class SparkDummy extends Callable[Int] {

  override def call(): Int = {

    val spark = SparkSession.builder().getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._

    spark.sparkContext.parallelize(Seq(1,2,3,4)).toDF.show()

    0
  }
}
