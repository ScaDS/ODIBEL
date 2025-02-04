package ai.scads.odibel.datasets.wikitext.utils

import ai.scads.odibel.datasets.wikitext.data.PageRevisionXmlSplit
import org.apache.spark.sql.SparkSession

/**
 * Util class to split a dump further requires FlatRawPageRevision
 */
class FlatPageRevisionPartitioner {

  def run(in: String, out: String, parts: Int) = {

    val spark = SparkSession.builder().getOrCreate()

    val sparkSql = spark.sqlContext
    import sparkSql.implicits._

    sparkSql.read
      .json(in)
      .as[PageRevisionXmlSplit]
      .map(WikiUtil.enrichFlatRawPageRevision)
      .repartition(parts,$"pid")
      .sortWithinPartitions($"pid",$"rid")
      .write.json(out)
  }
}
