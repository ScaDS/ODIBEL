package ai.scads.odibel.datasets.wikitext

import org.apache.spark.sql.SparkSession

class FlatPageRevisionPartitioner {

  def run(in: String, out: String, parts: Int) = {

    val spark = SparkSession.builder().getOrCreate()

    val sparkSql = spark.sqlContext
    import sparkSql.implicits._

    sparkSql.read
      .json(in)
      .as[FlatRawPageRevision]
      .map(WikiUtil.enrichFlatRawPageRevision)
      .repartition(parts,$"pid")
      .sortWithinPartitions($"pid",$"rid")
      .write.json(out)
  }
}
