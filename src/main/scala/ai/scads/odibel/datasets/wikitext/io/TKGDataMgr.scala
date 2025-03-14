package ai.scads.odibel.datasets.wikitext.io

import ai.scads.odibel.config.Config
import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.tansform.SerUtil
import ai.scads.odibel.main.DBpediaTKG.TemporalExtraction
import org.apache.spark.sql.SparkSession

class TKGDataMgr {

  def parseSpark(in: String, out: String, oFormat: String): Unit = {

    val spark = SparkSession.builder().config(Config.Spark.getConfig).getOrCreate()
    val sql = spark.sqlContext

    import sql.implicits._

    oFormat match {
      case "nquads" =>
        sql.read.parquet(in).withColumn("tFrom", $"tFrom".cast("long")).withColumn("tUntil", $"tUntil".cast("long")).as[TemporalExtractionResult].flatMap(SerUtil.buildQuads).write.option("compression", "bzip2").text(out)
      case "csv" =>
        System.err.println("TODO")
      case _ =>
        System.err.println("Format not implemented")
    }
  }

}
