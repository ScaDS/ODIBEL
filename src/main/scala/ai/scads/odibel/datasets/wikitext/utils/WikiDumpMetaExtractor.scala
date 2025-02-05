package ai.scads.odibel.datasets.wikitext.utils

import ai.scads.odibel.datasets.wikitext.data.PageRevision
import upickle.default._

import scala.xml.XML

/**
 * De-normalization/flattening of each page revision element
 * TODO wrong code, bad functionality
 */
class WikiDumpMetaExtractor {

  // TODO wrong case class name
  case class PageRevisionShort(pageid: Long, title: String, id: Long, oldid: Option[Long], timestamp: String, namespace: Int)

  implicit val ownerRw: ReadWriter[PageRevisionShort] = macroRW

  private def writeTo(pageXmlString: String, revisionXmlString: String): String = {
    val pageXml = XML.loadString(pageXmlString)
    val revisionXML = XML.loadString(revisionXmlString)

    val parentid = {
      val optParentid = (revisionXML \ "parentid").text
      if (optParentid == "") None
      else Some(optParentid.toLong)
    }
    write(PageRevisionShort(
      pageid = (pageXml \ "id").text.toLong,
      title = (pageXml \ "title").text,
      id = (revisionXML \ "id").text.toLong,
      oldid = parentid,
      timestamp = (revisionXML \ "timestamp").text,
      namespace = (pageXml \ "ns").text.toInt
    ))
  }

  def run(): Unit = {
    Iterator.continually(scala.io.StdIn.readLine()).takeWhile(_ != null).map({
      line =>
        val wikitext = read[PageRevision](line)(macroRW[PageRevision])
    }).foreach(record => {
      println(record)
    })
  }
}

object WikiDumpMetaExtractor extends App {

  val p = new WikiDumpMetaExtractor
  p.run()
}
