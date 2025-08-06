package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.DBpediaTKGExtraction.processPageRevisionIterator
import ai.scads.odibel.datasets.wikitext.config.ProfileConfig
import ai.scads.odibel.datasets.wikitext.data.{PageRevision, TemporalExtractionResult}
import ai.scads.odibel.datasets.wikitext.utils.WikiUtil
import ai.scads.odibel.utils.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory

import java.io.{File, InputStream}
import scala.io.{BufferedSource, Source}

/**
 * Simple class to extract a DBpedia TKG without SPARK using multiple threads of a single JVM
 */
class DBpediaTKGExtraction {

  def processStream(iterator: Iterator[String], diefUrl: String): Iterator[TemporalExtractionResult] = {
    processPageRevisionIterator(WikiUtil.splitToItem(iterator).map(WikiUtil.enrichFlatRawPageRevision), diefUrl)
  }

  def process(source: String, sink: String, endpoints: List[String]): Unit = {

    val iterator =
      if(source.startsWith("hdfs")){
        val hdfs = new HDFSUtil(source)
        val fs: FileSystem = hdfs.getFs
        val path = new Path(source)

        val files = if (fs.getFileStatus(path).isDirectory) {
          fs.listStatus(path).filter(_.isFile).map(_.getPath)
        } else {
          Array(path)
        }

        def openStream(p: Path): InputStream = {
          val codecFactory = new CompressionCodecFactory(new Configuration())
          val codec = codecFactory.getCodec(p)
          val inputStream = fs.open(p)
          if (codec != null) codec.createInputStream(inputStream) else inputStream
        }

        files.iterator.flatMap { p =>
          val src: BufferedSource = Source.fromInputStream(openStream(p))
          try src.getLines().toList.iterator
          finally src.close()
        }

      } else {
        val file = new File(source)
        val files = if (file.isDirectory) {
          file.listFiles().filter(_.isFile)
        } else {
          Array(file)
        }

        files.iterator.flatMap { f =>
          val src: BufferedSource = Source.fromFile(f)
          try src.getLines().toList.iterator
          finally src.close()
        }

      }

    /*
    val jobs = sourceFiles.map({
      sourceFile =>
        ExtractionJob(sourceFile,sink+"/"+sourceFile.split("/").last)
    })

    val monitor = new HeartbeatMonitor()
    val monitorThread = new Thread(monitor)
    monitorThread.start()
    val eventLogger = new EventLogger(monitor)

    val executor = new Executor(jobs, endpoints, eventLogger)
    monitor.registerRunnerThreads(executor.threads.toList)
    executor.start()

    monitor.show()
    executor.waitForCompletion()

     */


    val ters: Iterator[TemporalExtractionResult] = processStream(iterator, endpoints.head)

    import com.univocity.parsers.csv._
    // Create a CSV writer
    val writerSettings = new CsvWriterSettings()
    writerSettings.setHeaderWritingEnabled(true) // Include headers

    //        val stringWriter = new OutputStreamWriter(System.out)
    val csvWriter = new CsvWriter(System.out, writerSettings)

    // Write header row
    csvWriter.writeHeaders("head", "rel", "tail", "rStart", "rEnd", "tStart", "tEnd")

    ters.foreach(ter => {
      csvWriter.writeRow(ter.head, ter.rel, ter.tail, ter.rFrom, ter.rUntil, ter.tFrom, ter.tUntil)
    })
    csvWriter.flush()
  }
}

object DBpediaTKGExtraction {

  def processPageRevisionIterator(pageRevisionIterator: Iterator[PageRevision], diefEndpoint: String): Iterator[TemporalExtractionResult] = {
    val rc = new RCDiefServer(diefEndpoint)
    implicit var twb: TemporalWindowBuilder = new TemporalWindowBuilder()
    var oPageId = -1L

    pageRevisionIterator.flatMap({
      pageRevision =>
        if (ProfileConfig.wikiNamespaceFilter.contains(pageRevision.ns.getOrElse(-1))) {
          val tripleDoc = extractTripleDoc(rc, pageRevision)

          if (oPageId != pageRevision.pId) {
            // TODO eventLog.logEvent(SucceededPageEvent(pageId,Map()))
            oPageId = pageRevision.pId
            val temporalResults: List[TemporalExtractionResult] = twb.addGraphVersion(List(),Long.MaxValue)(Long.MaxValue.toString) // twb.buildEntries()
            twb = new TemporalWindowBuilder()
            diffAndAppendWindow(tripleDoc, pageRevision)
            temporalResults
          } else {
            diffAndAppendWindow(tripleDoc, pageRevision)
          }
        } else {
          None
        }
    }) ++ twb.addGraphVersion(List(),Long.MaxValue)(Long.MaxValue.toString) // TODO check
  }

  def diffAndAppendWindow(triples: Option[List[String]],pageRevision: PageRevision)(implicit twb: TemporalWindowBuilder): List[TemporalExtractionResult] = {
    if(triples.isDefined) {
      val ters = twb.addGraphVersion(triples.get,pageRevision.rTimestamp)(pageRevision.rId.toString)
      ters
    } else {
      // TODO this is where we skip bad revisions
      List()
    }
  }

  // TODO revision can brake so we decided to return None instead of empty String to not remove triples later
  def extractTripleDoc(rc: RCDiefServer, pageRevision: PageRevision): Option[List[String]] = {
    rc.extract(createBody(pageRevision.pagexml, pageRevision.revisionxml)) match {
      case Right(value) =>
        // TODO eventLog.logEvent(FailedRevisionEvent(wikitext.rId, "ERROR"))
        System.err.println(s"Exception @${pageRevision.pId}_${pageRevision.rId}: ${value.getMessage} ")
        None
      case Left(value) =>
        Some(value.split("\n").filter(_.startsWith("<")).toList)
      // TODO eventLog.logEvent(SucceededRevisionEvent(wikitext.rId, Map()))
    }
  }

  def createBody(pagexml: String, revisionxml: String): String = {
    val body = {
      "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
        "<page>\n" + pagexml + "<revision>\n" + revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
    }
    body
  }

}
