package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.config.Config
import ai.scads.odibel.datasets.wikitext.DBpediaTKGExtraction.processPageRevisionIterator
import ai.scads.odibel.datasets.wikitext.config.ProfileConfig
import ai.scads.odibel.datasets.wikitext.data.{PageRevision, PageRevisionXmlSplit, RevisionMeta, TemporalExtractionResult}
import ai.scads.odibel.datasets.wikitext.log.{FailedRevisionEvent, SkippedRevisionEvent, SucceededPageEvent, SucceededRevisionEvent}
import ai.scads.odibel.datasets.wikitext.utils.{SparkSessionUtil, WikiUtil}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File
import java.time.{Duration, Instant}
import java.util.concurrent.Callable
import scala.io.Source
import scala.xml.XML

// TODO check and write down todos

/**
 * Version that is using SPARK and HDFS entirely
 */
object DBpediaTKGExtractionSpark {

  private val session = SparkSessionUtil.spark
  private val sparkcontext = session.sparkContext
  private val sparksql = session.sqlContext

  import sparksql.implicits._

  def call(inPaths: List[String], sinkPath: String, diefEndpoints: List[String]): Int = {

    case class SplitRow(pagexml: String, revisionxml: String)

    val poolSize = diefEndpoints.length

    val assignedDumpsPathString = inPaths.zipWithIndex.map({
      case (filePath, idx) =>
        (filePath, diefEndpoints(idx % poolSize))
    })

    val dumpsRDD = sparksql.createDataset(assignedDumpsPathString).toDF("dumpPath", "dief")

    dumpsRDD.repartition(poolSize, $"dief").mapPartitions({
      rows: Iterator[Row] =>
        val rowList = rows.toList
        if (rowList.nonEmpty) {
          val diefEndpoint = rowList.head.getAs[String]("dief")
          val orderedPageRevisionIterator = readFilesSequentially(rowList.map(_.getAs[String]("dumpPath")))
          processPageRevisionIterator(orderedPageRevisionIterator, diefEndpoint)
        } else {
          Iterator.empty
        }
    }).write.mode("overwrite").parquet(sinkPath)

//    def processPageRevisionIterator(pageRevisionIterator: Iterator[PageRevision], diefEndpoint: String): Iterator[TemporalExtractionResult] = {
//      val rc = new RCDiefServer(diefEndpoint)
//      var tb = new TemporalWindowBuilder
//      var oPageId = -1L
//
//      pageRevisionIterator.flatMap({
//        pageRevision =>
//          if (ProfileConfig.wikiNamespaceFilter.contains(pageRevision.ns.getOrElse(-1))) {
//            val tripleDoc = extractTripleDoc(rc, pageRevision)
//
//            if (oPageId != pageRevision.pId) {
//              // TODO eventLog.logEvent(SucceededPageEvent(pageId,Map()))
//              oPageId = pageRevision.pId
//              val temporalResults: List[TemporalExtractionResult] = tb.buildEntries()
//
//
//              tb = new TemporalWindowBuilder()
//              tb.addGraphVersion(tripleDoc, pageRevision.rTimestamp)
//
//
//              temporalResults
//            } else {
//
//              None
//            }
//          } else {
//            None
//          }
//
//
//      }) ++ tb.buildEntries()
//    }



//    def doing(): Unit = {
//
//      val triples = rc.extract(createBody(pageRevision.pagexml, pageRevision.revisionxml)) match {
//        case Right(value) =>
//          System.err.println(s"Exception @${pageRevision.pId}_${pageRevision.rId}: ${value.getMessage} ")
//        // TODO eventLog.logEvent(FailedRevisionEvent(wikitext.rId, "ERROR"))
//        //                println(body) TODO
//        case Left(value) =>
//          val triples = value.split("\n").toList.filter(_.startsWith("<"))
//          tb.addGraphVersion(triples, pageRevision.rTimestamp)(pageRevision.rId.toString)
//        // TODO eventLog.logEvent(SucceededRevisionEvent(wikitext.rId, Map()))
//      }
//
//      if (oPageId != pageRevision.pId) {
//        // TODO eventLog.logEvent(SucceededPageEvent(pageId,Map()))
//        oPageId = pageRevision.pId
//        val temporalResults = tb.buildEntries()
//        tb = new TemporalWindowBuilder()
//        Some(temporalResults)
//      } else {
//        None
//      }
//    }

//    else
//    {
//      // TODO eventLog.logEvent(SkippedRevisionEvent(wikitext.rId))
//      None
//    }


    //              if (oPageId != pageRevision.pId) {
    //                // TODO eventLog.logEvent(SucceededPageEvent(pageId,Map()))
    //                oPageId = pageRevision.pId
    //                val temporalResults = tb.buildEntries()
    //                tb = new TemporalWindowBuilder()
    //                Some(temporalResults)
    //              } else {
    //                diefEndpoint.extract(createBody(pageRevision.pagexml, pageRevision.revisionxml)) match {
    //                  case Left(responseBody) =>
    //                    responseBody.split("\n").toList.filter(! _.trim.startsWith("#"))
    //
    //                    None
    //                  case Right(exception) =>
    //                    // TODO
    //                    None
    //                }
    //              }
    sparkcontext.stop()
    0
  }

  def dateToStamp(dateTime: String): Long = {
    Instant.parse(dateTime).getEpochSecond
  }


  class FileLinesIterator(file: File) extends Iterator[String] {

    private val source = Source.fromFile(file)
    private val lines = source.getLines()
    private var closed = false

    override def hasNext: Boolean = {
      if (closed) false
      else if (lines.hasNext) true
      else {
        close()
        false
      }
    }

    override def next(): String = {
      if (!hasNext) throw new NoSuchElementException("No more lines in file")
      val line = lines.next()
      if (!lines.hasNext) close() // Ensure file closes after last line
      line
    }

    private def close(): Unit = {
      if (!closed) {
        closed = true
        source.close()
      }
    }
  }

  def readFilesSequentially(filenames: List[String]): Iterator[PageRevision] = {
    filenames.iterator.flatMap { filename =>
      val fileLinesIterator = new FileLinesIterator(new File(filename))
      WikiUtil.splitToItem(fileLinesIterator).map(WikiUtil.enrichFlatRawPageRevision)
    }
  }



  //
  //    val session = SparkSession.builder().config(Config.Spark.getConfig).getOrCreate()
  //
  //    val sparkcontext = session.sparkContext
  //    sparkcontext.setLogLevel("WARN")
  //    val sparksql = session.sqlContext
  //
  //    import sparksql.implicits._
  //
  //    val gloobpath = inpath
  //    //  val df = sparksql.read.json("/home/marvin/workspace/data/pages_with_5_rev.split").as[SplitRow]
  //    val df = sparksql.read.json(gloobpath).as[FlatRawPageRevision]
  //    //  val df = sparksql.read.json("/home/marvin/workspace/data/test.split").as[FlatRawPageRevision]
  //
  //    val now = System.currentTimeMillis()
  //
  //    val temporalRecords = df.map(WikiUtil.enrichFlatRawPageRevision)
  //      .filter(_.ns.get == 0) // TODO
  //      //    .filter(_.pId > 18247 )
  //      .repartition(parallelism,partitionExprs = $"pId")
  //      .groupBy("pId")
  //      .as[Long,FlatPageRevision]
  //      .flatMapSortedGroups($"rId".asc)((x, revisions) => {
  //
  //        ////             Some(row.sliding(2).map(ar => ar.head.rId < ar.last.rId).reduce(_ && _))
  //        println("processing page "+x+" @"+System.currentTimeMillis()+"ms since epoch")
  //
  //        val ctx = TaskContext.get
  //        //      val stageId = ctx.stageId
  //        val partId = ctx.partitionId()
  //        //      val executorId = Thread.currentThread().getId
  //
  //
  //        val tgb = new TemporalWindowBuilder()
  //        val rc = new RCDiefServer(s"http://127.0.0.1:${diefStartPort+partId}/server/extraction/en/")
  //
  //        revisions.foreach({
  //          revision =>
  //            val body = {
  //              "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
  //                "<page>\n" + revision.pagexml + "<revision>\n" + revision.revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
  //            }
  //            //          println(body)
  //            rc.extract(body) match {
  //              case Right(value) =>
  //                System.err.println(s"Exception @${revision.pId}_${revision.rId}: ${value.getMessage} ")
  //                println(body)
  //              case Left(value) =>
  //                val triples = value.split("\n").toList
  //                tgb.addGraphVersion(triples.filter(_.startsWith("<")), revision.rTimestamp)(revision.rId.toString)
  //            }
  //        })
  //        tgb.buildEntries()
  //      })
  //
  //    temporalRecords.write.parquet(sinkpath)
  //
  //    val end = System.currentTimeMillis()
  //    println(Duration.ofMillis(end-now))
  //

}
