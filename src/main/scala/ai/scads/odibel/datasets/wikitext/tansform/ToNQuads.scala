package ai.scads.odibel.datasets.wikitext.tansform

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.tansform.Json2CSV.{args, sql}
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object ToNQuads extends App {


  val sql = SparkSessionUtil.sql

  import sql.implicits._

  if (args.length != 3)
    System.err.println("usage -- inputPath OutputPath")

  sql.read.parquet(args(0))
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .repartition(2048,$"head", $"tFrom")
    .as[TemporalExtractionResult]
    .flatMap({
      ter =>
        SerUtil.buildQuads(
          if (ter.tail.startsWith("\"")) ter
          else ter.copy(tail = ter.tail.dropRight(1))
        )
    })
      .write.mode("overwrite").option("compression", "bzip2").text(args(1))
}
