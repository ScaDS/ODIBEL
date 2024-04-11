package ai.scads.odibel

import org.apache.spark.sql.SparkSession

object WikiPageSeparator {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val sql = sparkSession.sqlContext

    val df = sql.read
      .format("xml")
      .option("rowTag", "page")
      .load("/media/marvin/tmp/workspace/enwiki-20240401-pages-meta-history1.xml-p1p823.bz2")

    df.show(20)

  }

}
