package ai.scads.odibel.datasets.wikitext.utils

//import ai.scads.odibel.datasets.dummy.SparkDummyJob.PageRevisionShort
import ai.scads.odibel.datasets.wikitext.data.{PageRevision, PageRevisionXmlSplit, RevisionMeta}

import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import scala.xml.XML

object WikiUtil {

  object Hashes {
    def sha256(input: String): Array[Byte] = {
      val md = MessageDigest.getInstance("SHA-256")
      val digest = md.digest(input.getBytes)
      digest
    }

    def md5(input: String): Array[Byte] = {
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(input.getBytes)
      digest
    }

    implicit class ByteArrayOps(bytes: Array[Byte]) {
      def toBase64: String = Base64.getEncoder.encodeToString(bytes)
      def toHex: String = bytes.map("%02x".format(_)).mkString
    }
  }

  def dateToStamp(dateTime: String): Long = {
    Instant.parse(dateTime).getEpochSecond
  }

  def mapSplitElementToRevisionElement(flatPageRevision: PageRevisionXmlSplit): RevisionMeta = {
    val pageXml = XML.loadString("<page>\n" + flatPageRevision.pagexml + "</page>")
    val pageTitle = (pageXml \ "title").text
    val pageID = (pageXml \ "id").text.toLong
    val namespace = (pageXml \ "ns").text.toInt
//    val redirect_title = (pageXml \ "redirect" \ "@title").text

    val revisionXml = XML.loadString("<revision>\n" + flatPageRevision.revisionxml + "</revision>")
    val revisionID = (revisionXml \ "id").text.toLong
    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
    val wikitext = (revisionXml \ "text").text

    RevisionMeta(pageTitle, pageID, namespace, revisionID, revisionTimestamp, wikitext)
  }

  def enrichFlatRawPageRevision(flatPageRevision: PageRevisionXmlSplit): PageRevision = {
    val pageXml = XML.loadString("<page>\n" + flatPageRevision.pagexml + "</page>")
    //    val pageTitle = (pageXml \ "title").text
    val pageID = (pageXml \ "id").text.toLong
    val namespace = (pageXml \ "ns").text.toInt
    //    val redirect_title = (pageXml \ "redirect" \ "@title").text

    val revisionXml = XML.loadString("<revision>\n" + flatPageRevision.revisionxml + "</revision>")
    val revisionID = (revisionXml \ "id").text.toLong
    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
    //    val wikitext = (revisionXml \ "text").text

    PageRevision(pageID, revisionID, revisionTimestamp, flatPageRevision.pagexml, flatPageRevision.revisionxml, Some(namespace))
  }

  def splitToItem(iterator: Iterator[String]): Iterator[PageRevisionXmlSplit] = {
    val page_header_buff = new StringBuilder()
    val page_revision_buff = new StringBuilder()
    var in_page_header = false
    var in_revision = false
    iterator.takeWhile(_ != null).flatMap({
      line =>
        line.trim match {
          case "<page>" =>
            in_page_header = true
            None
          case "</page>" =>
            page_header_buff.setLength(0)
            None
          case "<revision>" =>
            in_page_header = false
            in_revision = true
            None
          case "</revision>" =>
            in_revision = false
            val page_revision = page_revision_buff.toString
            page_revision_buff.setLength(0)
            // TODO conversionFunction to case class
            //  Missing Ns and so on
            Some(PageRevisionXmlSplit(page_header_buff.toString, page_revision))
          case _ =>
            if (in_page_header) page_header_buff.append(line + "\n")
            else if (in_revision) page_revision_buff.append(line + "\n")
            None
        }
    })
  }
}
