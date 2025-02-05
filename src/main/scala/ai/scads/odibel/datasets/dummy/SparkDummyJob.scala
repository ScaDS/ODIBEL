package ai.scads.odibel.datasets.dummy

import ai.scads.odibel.datasets.wikitext.RCDiefServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, functions}
import upickle.legacy.{ReadWriter, macroRW, write}

import scala.jdk.CollectionConverters._
import java.io.File
import scala.io.Source

object SparkDummyJob extends App {

  println(args.mkString(" "))

  val spark = {
    if(System.getenv().get("SPARK_MASTER").isEmpty)
      SparkSession.builder().appName("DummyJob").getOrCreate()
    else
      SparkSession.builder().master("local[*]").appName("DummyJob").getOrCreate()
  }
  val context = spark.sparkContext
  context.setLogLevel("WARN")
  val sql = spark.sqlContext
  import spark.implicits._

  val dirPathString=args(0)
  val dirFile= new File(dirPathString)

  val dumpsPathString = dirFile.listFiles().toList.map(_.getAbsolutePath)

  val assingSeq =
  Array(
    "http://localhost:59001/server/extraction/en",
    "http://localhost:59002/server/extraction/en",
    "http://localhost:59003/server/extraction/en",
    "http://localhost:59004/server/extraction/en"
  ).toList

  val poolSize = assingSeq.length

 val assignedDumpsPathString = dumpsPathString.zipWithIndex.map({
   case (filePath,idx) =>
     (filePath, assingSeq(idx % poolSize))
 })

  val dumpsRDD = spark.sqlContext.createDataset(assignedDumpsPathString).toDF("dumpPath","dief")

  import scala.io.Source
  import java.io.File

  case class PageRevisionShort(pagexml: String, revisionxml: String)
  //  case class PageRevisionData(pid, rid, )
  // TODO case class FlatPageRevision(pId: Long, rId: Long, rTimestamp: Long, pagexml: String, revisionxml: String, ns: Option[Int] = None)


  implicit val ownerRw: ReadWriter[PageRevisionShort] = macroRW

  private def writeTo(pageXmlString: String, revisionXmlString: String): String = {
    //    val pageXml = XML.loadString("<page>\n" + pageXmlString + "</page>\n")
    //    val revisionXML = XML.loadString("<revision>\n" + revisionXmlString + "</revision>\n")

    //    val parentid = {
    //      val optParentid = (revisionXML \ "parentid").text
    //      if (optParentid == "") None
    //      else Some(optParentid.toLong)
    //    }
    //    write(PageRevisionShort(
    //      pageid = (pageXml \ "id").text.toLong,
    //      title = (pageXml \ "title").text,
    //      id = (revisionXML \ "id").text.toLong,
    //      oldid = parentid,
    //      timestamp = (revisionXML \ "timestamp").text,
    //      revision = "<revision>\n" + revisionXmlString + "</revision>\n"))
    write(PageRevisionShort(pageXmlString, revisionXmlString)) + "\n"
  }

  def splitToItem(iterator: Iterator[String]): Iterator[PageRevisionShort] = {
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
            Some(PageRevisionShort(page_header_buff.toString, page_revision))
          case _ =>
            if (in_page_header) page_header_buff.append(line + "\n")
            else if (in_revision) page_revision_buff.append(line + "\n")
            None
        }
    })
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


  def readFilesSequentially(filenames: List[String]): Iterator[PageRevisionShort] = {
    filenames.iterator.flatMap { filename =>
      val fileLinesIterator = new FileLinesIterator(new File(filename))
      splitToItem(fileLinesIterator)
/*      val source = Source.fromFile(filename)
      try {
        source.getLines()
      } finally {
        // The closing of the source is handled lazily by wrapping it in an iterator.
        // source.close()
      }*/
    }
  }

  def createBody(pagexml: String, revisionxml: String): String = {
    val body = {
      "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">" +
        "<page>\n" + pagexml + "<revision>\n" + revisionxml + "</revision>\n" + "</page>\n</mediawiki>"
    }
    body
  }

  dumpsRDD.repartition(assingSeq.length, $"dief").mapPartitions({
    rows: Iterator[Row] =>
      val rowList = rows.toList
      if (rowList.nonEmpty) {
        val diefEndpoint = new RCDiefServer(rowList.head.getAs[String]("dief"))
        println("Runnign DIEFRCServer")
        readFilesSequentially(rowList.map(_.getAs[String]("dumpPath"))).map({
          pageRevision =>
            diefEndpoint.extract(createBody(pageRevision.pagexml, pageRevision.revisionxml)) match {
              case Left(responseBody) =>
                Some(responseBody.split("\n").toList.filter(! _.trim.startsWith("#")))
              case Right(exception) =>
                None
            }
        })
      } else {
        Iterator.empty
      }
  }).write.mode("overwrite").json("/home/marvin/paper/dbpedia-tkg/testdata/out")


/*  val rdd: RDD[Int] = spark.sparkContext.parallelize((0 to 100).toSeq)

  val df = sql.createDataset(rdd).toDF("id")

  df.agg(functions.sum("id")).show()*/
}
