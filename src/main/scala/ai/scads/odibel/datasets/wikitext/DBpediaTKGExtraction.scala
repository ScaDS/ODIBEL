package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.utils.ThroughputMonitor

import java.io.{File, FileWriter}
import scala.io.Source
import upickle.default._

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.{Executors, LinkedBlockingQueue, Semaphore, TimeUnit}
import scala.concurrent.Future
import scala.xml.XML

class DBpediaTKGExtraction {

  def runPath(path: String, sinkpath: String, diefUrls: List[String]): Unit = {
    val file = new File(path)
    val filepaths: List[String] = {
      if (file.isDirectory) {
        Option(file.listFiles()).getOrElse(Array.empty).map(_.getPath).toList
      } else List(path)
    }

    val pool = Executors.newFixedThreadPool(diefUrls.size)
    val query = new LinkedBlockingQueue[String]()
    filepaths.foreach(query.put)

    val tm = new ThroughputMonitor()

    val futures = diefUrls.map { diefUrl =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          try {
            while (true) {
              val filepath = query.poll(1, TimeUnit.SECONDS)
              if (filepath == null) {
                // No more files to process
                return
              }
              executeFileProcessing(
                filepath,
                Paths.get(sinkpath, new File(filepath).getName).toString,
                diefUrl,
                tm
              )
            }
          } catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt() // Preserve interrupt status
            case e: Exception =>
              e.printStackTrace() // Handle other exceptions
          }
        }
      })
    }

    println("init finished")
    pool.awaitTermination(1, TimeUnit.DAYS)
  }

  def writeOut(records: List[TemporalExtractionResult], fw: FileWriter): Unit = {
    import upickle.legacy._
    implicit val ownerRw: ReadWriter[TemporalExtractionResult] = macroRW

    records.foreach({
      record =>
        val line = write(record)+"\n"
        fw.write(line)
    })
    fw.flush()
  }

  def executeFileProcessing(filepath: String, sinkpath: String, diefUrl: String, tm: ThroughputMonitor): Unit = {

    val source = Source.fromFile(new File(filepath))
    val sink = new FileWriter(new File(sinkpath))

    val rc = new RCDiefServer(diefUrl)
    var tb = new TemporalWindowBuilder

    val dateString = "2002-02-25T15:43:11Z"
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))


    var oPageId = -1L
    var oRevisionId = -1L

    source.getLines()
      .foreach {
        line =>
          val wikitext = read[FlatPageRevision](line)(macroRW[FlatPageRevision])
//          val wikitext = read[Revision](line)(macroRW[Revision])
          val pageXml = XML.loadString("<page>\n" + wikitext.pagexml + "</page>\n")
//          val pageId = (pageXml \ "id").text.toLong
          val pageId = wikitext.pId
          if (oPageId != pageId) {
            println(s"next page $pageId")
            oPageId = pageId

            writeOut(tb.buildEntries(),sink)

            sink.flush()
            tb = new TemporalWindowBuilder()
          }

          val revisionXml = XML.loadString("<revision>\n" + wikitext.revisionxml + "</revision>\n")
//          val revisionId = (revisionXml \ "id").text
          val revisionId = wikitext.rId
          val timestampDateString = (revisionXml \ "timestamp").text
          val timestamp = sdf.parse(timestampDateString).getTime

          val mark = tm.markAndGet()
          if (mark % 100 == 0) println(s"$mark events ${tm.getThroughput} e/s")

          val body = {
            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
              "<page>\n" + wikitext.pagexml + "<revision>\n" + wikitext.revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
          }

          val triples = rc.extract(body).split("\n").toList
          tb.addGraphVersion(triples.filter(_.startsWith("<")), timestamp)(revisionId.toString)
      }

    val records = tb.buildEntries()
    //    println(quads)
    writeOut(records,sink)

    println("done wrote all")

    source.close()
    sink.close()
    0
  }
}
