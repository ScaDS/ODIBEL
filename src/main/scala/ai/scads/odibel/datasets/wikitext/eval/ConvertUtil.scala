package ai.scads.odibel.datasets.wikitext.eval

import ai.scads.odibel.datasets.wikitext.TemporalExtractionResult

object ConvertUtil extends App {

  val sql = EvalSpark.sql
  import sql.implicits._

  if(args.length != 3)
    System.err.println("usage -- inputPath OutputPath rePartitions")

  sql.read.json(args(0))
//    .withColumn("tFrom", $"tFrom".cast("long"))
//    .withColumn("tUntil", $"tUntil".cast("long"))
//    .as[TemporalExtractionResult]
    .repartition(args(2).toInt)
    .write.parquet(args(1))
}
