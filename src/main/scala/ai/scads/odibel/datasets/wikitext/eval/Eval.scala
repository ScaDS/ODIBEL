package ai.scads.odibel.datasets.wikitext.eval
import ai.scads.odibel.datasets.wikitext.{FlatRawPageRevision, TemporalExtractionResult, WikiUtil}
import org.apache.spark.sql.SparkSession

object Eval extends App {

  case class RevisionMetadata(pid: Long, rid: Long, timestamp: Long, ns: Int)

  val spark = SparkSession.builder()
    .config("spark.hadoop.fs.defaultFS","hdfs://athena1.informatik.intern.uni-leipzig.de:9000/")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val sql = spark.sqlContext
  import sql.implicits._

  def extractPageMetaTable(): Unit = {
    val df = sql.read.json(args(0)).repartition(2048)
    val mapped = df.as[FlatRawPageRevision].map({
      raw =>
        val enriched = WikiUtil.enrichFlatRawPageRevision(raw)
        RevisionMetadata(enriched.pId, enriched.rId, enriched.rTimestamp, enriched.ns.get)
    })
    mapped.repartition(64).write.mode("overwrite").parquet(args(1))
  }

  extractPageMetaTable()
}
