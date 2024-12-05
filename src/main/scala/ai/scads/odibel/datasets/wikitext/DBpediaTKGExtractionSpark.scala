package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.config.Config
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

import java.time.{Duration, Instant}
import scala.xml.XML

// TODO

/**
 * Version that is using SPARK and HDFS entirely
 */
object DBpediaTKGExtractionSpark extends App {

  case class SplitRow(pagexml: String, revisionxml: String)
  case class InfoObj(stageId: Int, partitionId: Int, executorId: Long)

  def dateToStamp(dateTime: String): Long = {
    Instant.parse(dateTime).getEpochSecond
  }

  def mapSplitElementToRevisionElement(splitRow: SplitRow): Revision = {
    val pageXml = XML.loadString("<page>\n" + splitRow.pagexml + "</page>")
    val pageTitle = (pageXml \ "title").text
    val pageID = (pageXml \ "id").text.toLong
    val namespace = (pageXml \ "ns").text
//    val redirect_title = (pageXml \ "redirect" \ "@title").text

    val revisionXml = XML.loadString("<revision>\n" + splitRow.revisionxml + "</revision>")
    val revisionID = (revisionXml \ "id").text.toLong
    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
    val wikitext = (revisionXml \ "text").text

    Revision(pageTitle, pageID, namespace.toInt, revisionID, revisionTimestamp, wikitext)
  }

  val session = SparkSession.builder().config(Config.Spark.getConfig).getOrCreate()

  val sparkcontext = session.sparkContext
  val sparksql = session.sqlContext

  import sparksql.implicits._

  //  val df = sparksql.read.json("/home/marvin/workspace/data/pages_with_5_rev.split").as[SplitRow]
  val df = sparksql.read.json("/home/marvin/workspace/data/wikidumps/enwiki-20240801-pages-meta-history1.xml-p17910p18672.split").as[FlatRawPageRevision]
//  val df = sparksql.read.json("/home/marvin/workspace/data/test.split").as[FlatRawPageRevision]

  val now = System.currentTimeMillis()

  val temporalRecords = df.map(WikiUtil.enrichFlatRawPageRevision)
    .filter(_.ns.get == 0) // TODO
//    .filter(_.pId > 18247 )
    .repartition(8, partitionExprs = $"pId")
    .groupBy("pId")
    .as[Long,FlatPageRevision]
    .flatMapSortedGroups($"rId".asc)((x, revisions) => {

////             Some(row.sliding(2).map(ar => ar.head.rId < ar.last.rId).reduce(_ && _))
        println("processing page "+x+" @"+System.currentTimeMillis()+"ms since epoch")

      val ctx = TaskContext.get
//      val stageId = ctx.stageId
      val partId = ctx.partitionId()
//      val executorId = Thread.currentThread().getId


      val tgb = new TemporalWindowBuilder()
      val rc = new RCDiefServer(s"http://127.0.0.1:${9500+partId}/server/extraction/en/")

      revisions.foreach({
        revision =>
          val body = {
            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
              "<page>\n" + revision.pagexml + "<revision>\n" + revision.revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
          }
//          println(body)
          rc.extract(body) match {
            case Right(value) =>
              System.err.println(s"Exception @${revision.pId}_${revision.rId}: ${value.getMessage} ")
              println(body)
            case Left(value) =>
              val triples = value.split("\n").toList
              tgb.addGraphVersion(triples.filter(_.startsWith("<")), revision.rTimestamp)(revision.rId.toString)
          }
      })
      tgb.buildEntries()
    })

  temporalRecords.write.mode("overwrite").json("/home/marvin/workspace/data/test.split.tkg2")

  val end = System.currentTimeMillis()
  println(Duration.ofMillis(end-now))

  sparkcontext.stop()
}
