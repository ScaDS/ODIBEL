package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil

object Json2CSV extends App {

  val sql = SparkSessionUtil.sql
  import sql.implicits._

  if(args.length != 3)
    System.err.println("usage -- inputPath OutputPath")

  sql.read.parquet(args(0))
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .as[TemporalExtractionResult]
    .map({
      ter =>
        if (ter.tail.startsWith("\"")) ter
        else ter.copy(tail = ter.tail.dropRight(1))
    })
    .show(truncate = false)
}

