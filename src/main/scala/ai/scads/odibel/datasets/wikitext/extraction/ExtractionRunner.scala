package ai.scads.odibel.datasets.wikitext.extraction

import ai.scads.odibel.datasets.wikitext.log.{EventLogger, FailedRevisionEvent, SkippedRevisionEvent, SucceededPageEvent, SucceededRevisionEvent}
import ai.scads.odibel.datasets.wikitext.{FlatPageRevision, RCDiefServer, TemporalExtractionResult, TemporalWindowBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import upickle.default._

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.io.Source


// Runs on a single File communicates with a set (1) DIEF Server
class ExtractionRunner(extractionJob: ExtractionJob, endpoint: String, eventLog: EventLogger) {

  val dateString = "1996-05-21T15:43:11Z"
  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  private val uri = new URI(extractionJob.sourceUri)
  private val scheme = Option(uri.getScheme)

  val hdfsConfiguration = new Configuration()
  private val fs = scheme match {
    case Some("hdfs") =>
      FileSystem.get(uri, hdfsConfiguration)
    case Some("file") | None =>
      null // Local file system, FileSystem not needed
    case _ =>
      throw new IllegalArgumentException(s"Unsupported scheme: ${uri.getScheme}")
  }

  val sourceIterator: Iterator[String] = scheme match {
    case Some("hdfs") =>
      val path = new Path(extractionJob.sourceUri)

      if (!fs.exists(path)) {
        throw new IllegalArgumentException(s"File not found: ${extractionJob.sourceUri}")
      }
      // Automatically detect the compression codec
      val codecFactory = new CompressionCodecFactory(hdfsConfiguration)
      val codec = codecFactory.getCodec(path)

      // Open file stream, applying codec if compressed
      val inputStream = if (codec != null) {
        codec.createInputStream(fs.open(path))
      } else {
        fs.open(path)
      }
      // Return an iterator for the lines
      Source.fromInputStream(inputStream).getLines()

    case _ =>
      val source = Source.fromFile(new File(extractionJob.sourceUri))
      source.getLines()
  }

  def openOutputStream(): OutputStream = {
    scheme match {
      case Some("hdfs") =>
        val path = new Path(extractionJob.sinkUri)

        // Automatically detect the compression codec based on the file extension
        val codecFactory = new CompressionCodecFactory(hdfsConfiguration)
        val codec = codecFactory.getCodec(path)

        // Create output stream
        if (codec != null) {
          codec.createOutputStream(fs.create(path, true))
        } else {
          fs.create(path, true)
        }
      case _ =>
        new FileOutputStream(new File(extractionJob.sinkUri))
    }
  }

  def writeOut(records: List[TemporalExtractionResult], os: BufferedOutputStream): Unit = {
    implicit val ownerRw: ReadWriter[TemporalExtractionResult] = macroRW

    records.foreach({
      record =>
        val line = write(record) + "\n"
        os.write(line.getBytes(StandardCharsets.UTF_8))
    })
    os.flush()
  }

  def execute(): Unit = {

    val rc = new RCDiefServer(endpoint)
    var tb = new TemporalWindowBuilder

    var oPageId = -1L

    val os = new BufferedOutputStream(openOutputStream())

    sourceIterator
      .foreach {
        line =>
          val wikitext = read[FlatPageRevision](line)(macroRW[FlatPageRevision])
          val pageId = wikitext.pId
          if (oPageId != pageId) {
            eventLog.logEvent(SucceededPageEvent(pageId,Map()))
            oPageId = pageId
            writeOut(tb.buildEntries(), os)
            os.flush()
            tb = new TemporalWindowBuilder()
          }

          //          val revisionXml = XML.loadString("<revision>\n" + wikitext.revisionxml + "</revision>\n")
          //          val revisionId = wikitext.rId
          //          val timestampDateString = (revisionXml \ "timestamp").text
          //          val timestamp = sdf.parse(timestampDateString).getTime

          //          val mark = tm.markAndGet()
          //          if (mark % 10000 == 0) println(s"$mark events ${tm.getThroughput} e/s")

          val body = {
            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
              "<page>\n" + wikitext.pagexml + "<revision>\n" + wikitext.revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
          }

          if (wikitext.ns.get == 0 || wikitext.ns.get == 14) {
            rc.extract(body) match {
              case Right(value) =>
                System.err.println(s"Exception @${wikitext.pId}_${wikitext.rId}: ${value.getMessage} ")
                eventLog.logEvent(FailedRevisionEvent(wikitext.rId, "ERROR"))
//                println(body) TODO
              case Left(value) =>
                val triples = value.split("\n").toList
                tb.addGraphVersion(triples.filter(_.startsWith("<")), wikitext.rTimestamp)(wikitext.rId.toString)
                eventLog.logEvent(SucceededRevisionEvent(wikitext.rId, Map()))
            }
          } else {
            eventLog.logEvent(SkippedRevisionEvent(wikitext.rId))
          }
      }

    val records = tb.buildEntries()

    writeOut(records, os)
    os.close()
  }
}
