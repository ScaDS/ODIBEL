package ai.scads.odibel.main

import ai.scads.odibel.datasets.wikitext.DBpediaTKGExtraction.getClass
import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.transform.SerUtil
import ai.scads.odibel.datasets.wikitext.utils.{FlatPageRevisionPartitioner, WikiDumpFlatter}
import ai.scads.odibel.datasets.wikitext.{DBpediaTKGExtraction, DBpediaTKGExtractionSpark}
import ai.scads.odibel.main.DBpediaTKG.{FlatRepartitioner, TemporalExtraction, WikidumpRevisionSplit}
import org.slf4j.LoggerFactory
import picocli.CommandLine.{Command, Option}

import java.io.{File, FileInputStream, FileOutputStream, InputStream, OutputStream, OutputStreamWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable
import scala.io.Source
import scala.jdk.CollectionConverters._

object DBpediaTKG {

  private val logger = LoggerFactory.getLogger(classOf[DBpediaTKG])

  @Command(name = "split")
  class WikidumpRevisionSplit extends Callable[Int] {

    @Option(names = Array("-i"))
    var inFile: File = _

    @Option(names = Array("-o"))
    var outFile: File = _

    override def call(): Int = {
      System.err.println(s"in: ${inFile.getPath} out: ${outFile.getPath}")

      val revSplitter = new WikiDumpFlatter()
      revSplitter.run(inFile, outFile)
      0
    }
  }


  @Command(name = "extractSpark")
  class TemporalExtractionSpark extends Callable[Int] {

    @Option(names = Array("-i"))
    var in: String = _

    @Option(names = Array("-o"))
    var out: String = _

    @Option(names = Array("-e"), split = ",")
    var diefEndpoints: java.util.ArrayList[String] = _


    override def call(): Int = {

      val urls = parseEndpointPatternList(diefEndpoints)

      val extraction = DBpediaTKGExtractionSpark
      extraction.call(List(in), out, urls)
    }
  }

  def parseEndpointPatternList(diefEndpoints: java.util.ArrayList[String]): List[String] = {
    val portRegex = ":(\\d+)-(\\d+)".r

    diefEndpoints.toArray.flatMap({
      case diefEndpoint: String =>
        val matches = portRegex.findFirstMatchIn(diefEndpoint)
        if (matches.isDefined) {
          val startPort = matches.get.group(1).trim.toInt
          val urlPrefix = diefEndpoint.substring(0, matches.get.start)
          val urlSuffix = diefEndpoint.substring(matches.get.end)
          val endPort = matches.get.group(2).trim.toInt
          (startPort to endPort).map({
            port => urlPrefix + ":" + port + urlSuffix
          }).toList
        } else {
          List(diefEndpoint)
        }
    }).toList
  }

  @Command(name = "extract")
  class TemporalExtraction extends Callable[Int] {

    @Option(names = Array("-i"))
    var in: String = _

    @Option(names = Array("-o"))
    var out: String = _

    @Option(names = Array("-f"))
    var format: String = "csv"

    @Option(names = Array("-e"), split = ",")
    var diefEndpoints: java.util.ArrayList[String] = _

    override def call(): Int = {
      System.err.println(s"in: $in out: $out")

      logger.debug(s"in: $in out: $out")

      val extraction = new DBpediaTKGExtraction

      def inputStreamFrom(path: String): InputStream = {
        path match {
          case "-" =>
            System.in
          case p if p.startsWith("hdfs://") =>
            val fs = new org.apache.hadoop.fs.Path(p)
            val conf = new org.apache.hadoop.conf.Configuration()
            val fileSystem = fs.getFileSystem(conf)
            fileSystem.open(fs)
          case p =>
            new FileInputStream(p)
        }
      }

      def outputStreamTo(path: String): OutputStream = {
        path match {
          case "-" =>
            System.out
          case p if p.startsWith("hdfs://") =>
            val fs = new org.apache.hadoop.fs.Path(p)
            val conf = new org.apache.hadoop.conf.Configuration()
            val fileSystem = fs.getFileSystem(conf)
            if (fileSystem.exists(fs)) fileSystem.delete(fs, true)
            fileSystem.create(fs, true)
          case p =>
            new FileOutputStream(p)
        }
      }

      val inStream = inputStreamFrom(in)
      val outStream = outputStreamTo(out)

      try {
        val (ters, failedCount) =
          extraction.processStream(Source.fromInputStream(inStream).getLines(), diefEndpoints.asScala.head)

        logger.info(s"Number of revisions failed to extract: $failedCount")


        format match {
          case "csv" =>
            import com.univocity.parsers.csv._
            val writerSettings = new CsvWriterSettings()
            writerSettings.setHeaderWritingEnabled(true)

            val csvWriter = new CsvWriter(new OutputStreamWriter(outStream, StandardCharsets.UTF_8), writerSettings)
            csvWriter.writeHeaders("head", "rel", "tail", "rStart", "rEnd", "tStart", "tEnd")

            ters.foreach(ter =>
              csvWriter.writeRow(ter.head, ter.rel, ter.tail, ter.rStart, ter.rEnd, ter.tStart, ter.tEnd)
            )
            csvWriter.flush()

          case "ngraph" =>
            ters.flatMap(SerUtil.buildQuads).grouped(100).foreach(rows => {
              outStream.write((rows.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
              outStream.flush()
            })

          case value =>
            System.err.println(s"format $value not implemented")
        }
      } finally {
        if (in != "-") inStream.close()
        if (out != "-") outStream.close()
      }

      0
    }
  }
  @Command(name = "partition")
  class FlatRepartitioner extends Callable[Int] {

    @Option(names = Array("-i"), required = true)
    var inFile: File = _

    @Option(names = Array("-o"), required = true)
    var outFile: File = _

    @Option(names = Array("-p"), required = true)
    var numberOfPartitions: Int = _

    override def call(): Int = {
      System.err.println(s"in: ${inFile.getPath} out: ${outFile.getPath}")

      val repartitioner = new FlatPageRevisionPartitioner
      repartitioner.run(inFile.getPath, outFile.getPath, numberOfPartitions)
      0
    }
  }
}

@Command(
  name = "dbpedia-tkg",
  subcommands = Array(classOf[WikidumpRevisionSplit], classOf[TemporalExtraction], classOf[FlatRepartitioner]),
  mixinStandardHelpOptions = true
)
class DBpediaTKG extends Callable[Int] {

  override def call(): Int = {
    // Handled by subcommands
    0
  }
}

