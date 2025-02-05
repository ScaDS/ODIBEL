package ai.scads.odibel.datasets.wikitext.tansform

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object SerUtil {

  val TKG = "http://dbpedia.org/temporal"

  lazy val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  def formatDate(timestamp: Long): String = {
    try {
      Instant.ofEpochSecond(timestamp)
        .atOffset(ZoneOffset.UTC)
        .format(formatter)
    } catch {
      case e: Exception =>
        "9999-12-31T23:59:59"
    }
  }

  private def buildQuad(s: String, p: String, o: String, g: String): String = {
    s"<$s> <$p> " + {
      if (o.startsWith("\"")) s"$o " else s"<$o> "
    } + s"<$TKG/$g> ."

  }

  def buildQuads(ter: TemporalExtractionResult): List[String] = {
    List(
      buildQuad(ter.head, ter.rel, ter.tail, s"${ter.rFrom}-${ter.rUntil}"),
      buildQuad(s"$TKG/${ter.rFrom}-${ter.rUntil}", s"$TKG/start", s"\"${formatDate(ter.tFrom)}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", ""),
      buildQuad(s"$TKG/${ter.rFrom}-${ter.rUntil}", s"$TKG/end", s"\"${formatDate(ter.tUntil)}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", "")
    )
  }

}
