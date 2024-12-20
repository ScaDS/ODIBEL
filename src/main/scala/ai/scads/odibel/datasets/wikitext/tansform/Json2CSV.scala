package ai.scads.odibel.datasets.wikitext.tansform

import ai.scads.odibel.datasets.wikitext.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.EvalSpark

object Json2CSV extends App {

  val sql = EvalSpark.sql
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
//    .repartition(args(2).toInt,$"head",$"tFrom")
//    .sortWithinPartitions($"head",$"tFrom")
//    .write.mode("overwrite").option("compression", "bzip2").option("header","false").csv(args(1))
}

