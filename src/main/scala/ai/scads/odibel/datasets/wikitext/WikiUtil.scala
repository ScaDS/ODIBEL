package ai.scads.odibel.datasets.wikitext

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

  def mapSplitElementToRevisionElement(flatPageRevision: FlatRawPageRevision): Revision = {
    val pageXml = XML.loadString("<page>\n" + flatPageRevision.pagexml + "</page>")
    val pageTitle = (pageXml \ "title").text
    val pageID = (pageXml \ "id").text.toLong
    val namespace = (pageXml \ "ns").text.toInt
//    val redirect_title = (pageXml \ "redirect" \ "@title").text

    val revisionXml = XML.loadString("<revision>\n" + flatPageRevision.revisionxml + "</revision>")
    val revisionID = (revisionXml \ "id").text.toLong
    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
    val wikitext = (revisionXml \ "text").text

    Revision(pageTitle, pageID, namespace, revisionID, revisionTimestamp, wikitext)
  }

  def enrichFlatRawPageRevision(flatPageRevision: FlatRawPageRevision): FlatPageRevision = {
    val pageXml = XML.loadString("<page>\n" + flatPageRevision.pagexml + "</page>")
    //    val pageTitle = (pageXml \ "title").text
    val pageID = (pageXml \ "id").text.toLong
    val namespace = (pageXml \ "ns").text.toInt
    //    val redirect_title = (pageXml \ "redirect" \ "@title").text

    val revisionXml = XML.loadString("<revision>\n" + flatPageRevision.revisionxml + "</revision>")
    val revisionID = (revisionXml \ "id").text.toLong
    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
    //    val wikitext = (revisionXml \ "text").text

    FlatPageRevision(pageID, revisionID, revisionTimestamp, flatPageRevision.pagexml, flatPageRevision.revisionxml, Some(namespace))
  }
}
