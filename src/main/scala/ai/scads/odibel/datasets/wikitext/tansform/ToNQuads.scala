package ai.scads.odibel.datasets.wikitext.tansform

import ai.scads.odibel.datasets.wikitext.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.EvalSpark
import ai.scads.odibel.datasets.wikitext.tansform.Json2CSV.{args, sql}
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object ToNQuads extends App {

  val TKG = "http://dbpedia.org/temporal"

  val sql = EvalSpark.sql

  import sql.implicits._

  if (args.length != 3)
    System.err.println("usage -- inputPath OutputPath")

  lazy val formatter = DateTimeFormatter.ofPattern("YYYY-MM-DD'T'hh:mm:ss")

  def formatDate(timestamp: Long): String = {
    Instant.ofEpochSecond(timestamp)
      .atOffset(ZoneOffset.UTC)
      .format(formatter)
  }

  def buildQuad(s: String, p: String, o: String, g: String): String = {
    s"<$s> <$p> " + {
      if (o.startsWith("\"")) s"$o " else s"<$o> "
    } + s"<$TKG/$g> ."

  }

  def buildQuads(ter: TemporalExtractionResult): List[String] = {
    List(
      buildQuad(ter.head, ter.rel, ter.tail, s"${ter.rFrom}-${ter.rUntil}"),
      buildQuad(s"$TKG/${ter.rFrom}-${ter.rUntil}", s"$TKG/start", s"\"${formatDate(ter.tFrom)}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", ""),
      buildQuad(s"$TKG/${ter.rFrom}-${ter.rUntil}", s"$TKG/end", s"\"${formatDate(ter.tFrom)}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", "")
    )
  }

  sql.read.parquet(args(0))
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .repartition(2048,$"head", $"tFrom")
    .as[TemporalExtractionResult]
    .flatMap({
      ter =>
        buildQuads(
          if (ter.tail.startsWith("\"")) ter
          else ter.copy(tail = ter.tail.dropRight(1))
        )
    })
      .write.mode("overwrite").option("compression", "bzip2").text(args(1))
}
