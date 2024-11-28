//package ai.scads.odibel.datasets.wikitext
//
//import ai.scads.odibel.config.Config
//import org.apache.spark.sql.SparkSession
//
//import java.time.Instant
//import scala.xml.XML
//
//// TODO
//
///**
// * Version that is using SPARK and HDFS entirely
// */
//object DBpediaTKGExtractionSpark extends App {
//
//  case class SplitRow(pagexml: String, revisionxml: String)
//
//  def dateToStamp(dateTime: String): Long = {
//    Instant.parse(dateTime).getEpochSecond
//  }
//
//  def mapSplitElementToRevisionElement(splitRow: SplitRow): Revision = {
//    val pageXml = XML.loadString("<page>\n" + splitRow.pagexml + "</page>")
//    val pageTitle = (pageXml \ "title").text
//    val pageID = (pageXml \ "id").text.toLong
//    val namespace = (pageXml \ "ns").text
//    val redirect_title = (pageXml \ "redirect" \ "@title").text
//
//    val revisionXml = XML.loadString("<revision>\n" + splitRow.revisionxml + "</revision>")
//    val revisionID = (revisionXml \ "id").text.toLong
//    val revisionTimestamp = dateToStamp((revisionXml \ "timestamp").text)
//    val wikitext = (revisionXml \ "text").text
//
//    Revision(pageTitle, pageID, revisionID, revisionTimestamp, wikitext)
//  }
//
//  val session = SparkSession.builder().config(Config.Spark.getConfig).getOrCreate()
//
//  val sparksql = session.sqlContext
//
//  import sparksql.implicits._
//
//  //  val df = sparksql.read.json("/home/marvin/workspace/data/pages_with_5_rev.split").as[SplitRow]
//  val df = sparksql.read.json("/home/marvin/workspace/data/test.split").as[SplitRow]
//
//  df.map(mapSplitElementToRevisionElement)
//    //    .filter(_.text) // TODO
//    .repartition(2, partitionExprs = $"pId")
//    .groupByKey(_.pId)
//    .flatMapSortedGroups($"rId".asc)((_, revisions) => {
//      //       Some(row.sliding(2).map(ar => ar.head.rId < ar.last.rId).reduce(_ && _))
//      val tgb = new TemporalWindowBuilder()
//      val rc = new RCDiefServer("http://localhost:9500/server/extraction/en")
//
//      revisions.foreach({
//        revision =>
//          val body = {
//            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
//              "<page>\n" + "<revision>\n" + "<id>" + revision.rId + "</id>\n<text>" + revision.text + "</text>" + "</revision>\n" + "</page>\n</mediawiki>"
//          }
//          val triples = rc.extract(body).split("\n").toList
//          tgb.addGraphVersion(triples.filter(_.startsWith("<")), revision.timestamp)(revision.rId.toString)
//      })
//      tgb.buildQuads()
//    }).show(truncate = false)
//}
