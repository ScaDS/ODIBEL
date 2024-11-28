package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.WikiUtil.dateToStamp
import org.apache.spark.sql.SparkSession

import scala.xml.XML

class FlatPageRevisionPartitioner {

  def run(in: String, out: String) = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val sparkSql = spark.sqlContext
    import sparkSql.implicits._

    sparkSql.read
      .json(in)
      .as[FlatRawPageRevision]
      .map(WikiUtil.enrichFlatRawPageRevision)
      .repartition(64,$"pid")
      .sortWithinPartitions($"pid",$"rid")
      .write.json(out)
  }
}
