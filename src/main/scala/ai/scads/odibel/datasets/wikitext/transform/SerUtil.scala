package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import com.opencsv.{CSVParserBuilder, CSVReaderBuilder}

import java.io.StringReader
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

  case class RDFTriple(
                        head: String,
                        rel: String,
                        literal: String,
                        langTag: Option[String],
                        tStart: String,
                        tEnd: String,
                        rStart: String,
                        rEnd: String
                      )

  def readCsvLine(line: String): Option[RDFTriple] = {
    val fields = parseCsvLine(line)

    if (fields == null || fields.length != 7) {
      println(s"Skipped line: \n$line\n")
      return None
    }

    val Array(headRaw, relRaw, tailRaw, tStart, tEnd, rStart, rEnd) = fields

    val head = headRaw.stripPrefix("\"").stripSuffix("\"")
    val rel = relRaw.stripPrefix("\"").stripSuffix("\"")

    val langIndex = tailRaw.lastIndexOf("@")
    val (rawLiteral, langTagOpt) =
      if (langIndex != -1 && tailRaw.substring(langIndex).matches("@[a-zA-Z\\-]+")) {
        (tailRaw.substring(0, langIndex), Some(tailRaw.substring(langIndex)))
      } else {
        (tailRaw, None)
      }

    val literal = rawLiteral
      .stripPrefix("\"")
      .stripSuffix("\"")
      .replace("\\n", "\n")
      .replace("\\t", "\t")
      .replace("\\\"", "\"")

    Some(RDFTriple(head, rel, literal, langTagOpt, formatDate(tStart.toLong), formatDate(tEnd.toLong), formatDate(rStart.toLong), formatDate(rEnd.toLong)))
  }

  private def parseCsvLine(line: String): Array[String] = {
    val parser = new CSVParserBuilder()
      .withSeparator(',')
      .withQuoteChar('"')
      .withEscapeChar('\\')
      .build()

    val reader = new CSVReaderBuilder(new StringReader(line))
      .withCSVParser(parser)
      .build()

    reader.readNext()
  }

}
