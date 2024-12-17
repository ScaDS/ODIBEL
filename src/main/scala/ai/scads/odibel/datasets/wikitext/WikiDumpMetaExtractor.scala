package ai.scads.odibel.datasets.wikitext


import ai.scads.odibel.utils.ThroughputMonitor
import upickle.default._

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source
import scala.xml.XML

/**
 * De-normalization/flattening of each page revision element
 */
class WikiDumpMetaExtractor {

  case class PageRevisionShort(pageid: Long, title: String, id: Long, oldid: Option[Long], timestamp: String, namespace: Int)

  implicit val ownerRw: ReadWriter[PageRevisionShort] = macroRW

  private def writeTo(pageXmlString: String, revisionXmlString: String): String = {
    val pageXml = XML.loadString(pageXmlString)
    val revisionXML = XML.loadString(revisionXmlString)
//    val pageXml = XML.loadString("<page>\n" + pageXmlString + "</page>\n")
//    val revisionXML = XML.loadString("<revision>\n" + revisionXmlString + "</revision>\n")

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
//    var page_header_buff = new StringBuilder()
//    var page_revision_buff = new StringBuilder()
//    var in_page_header = false
//    var in_revision = false

//    val tm = new ThroughputMonitor
//    val tmThread = new Thread(() => {
//      while (true) {
//        Thread.sleep(5000)
//        System.err.println(s"\r${tm.getThroughput} r/s")
//      }
//    })
//    tmThread.start()


    Iterator.continually(scala.io.StdIn.readLine()).takeWhile(_ != null).map({
      line =>
        val wikitext = read[FlatPageRevision](line)(macroRW[FlatPageRevision])


      //        line.trim match {
//          case "<page>" =>
//            in_page_header = true
//            None
//          case "</page>" =>
//            page_header_buff.setLength(0)
//            None
//          case "<revision>" =>
//            in_page_header = false
//            in_revision = true
//            None
//          case "</revision>" =>
//            in_revision = false
//            val page_revision = page_revision_buff.toString
//            page_revision_buff.setLength(0)
//            Some(writeTo(page_header_buff.toString, page_revision))
//          case _ =>
//            if (in_page_header) page_header_buff.append(line + "\n")
//            else if (in_revision) page_revision_buff.append(line + "\n")
//            None
//        }
    }).foreach(record => {
//      tm.mark()
      println(record)
    })

//    tmThread.interrupt()
  }
}

object WikiDumpMetaExtractor extends App {

  val p = new WikiDumpMetaExtractor
  p.run()
}
