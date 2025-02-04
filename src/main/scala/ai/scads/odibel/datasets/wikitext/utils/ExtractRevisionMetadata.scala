package ai.scads.odibel.datasets.wikitext.utils

import ai.scads.odibel.datasets.wikitext.data.PageRevisionXmlSplit
import org.apache.spark.sql.SparkSession

// TODO move
object ExtractRevisionMetadata extends App {

  case class RevisionMetadata(pid: Long, rid: Long, timestamp: Long, ns: Int)

  val spark = SparkSession.builder()
    .config("spark.hadoop.fs.defaultFS","hdfs://athena1.informatik.intern.uni-leipzig.de:9000/")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val sql = spark.sqlContext
  import sql.implicits._

  def extractPageMetaTable(): Unit = {
    val df = sql.read.json(args(0)).repartition(2048)
    val mapped = df.as[PageRevisionXmlSplit].map({
      raw =>
        val enriched = WikiUtil.enrichFlatRawPageRevision(raw)
        RevisionMetadata(enriched.pId, enriched.rId, enriched.rTimestamp, enriched.ns.get)
    })
    mapped.repartition(64).write.mode("overwrite").parquet(args(1))
  }

  extractPageMetaTable()
}
