package ai.scads.odibel.main

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.transform.SerUtil
import ai.scads.odibel.datasets.wikitext.utils.{FlatPageRevisionPartitioner, WikiDumpFlatter}
import ai.scads.odibel.datasets.wikitext.{DBpediaTKGExtraction, DBpediaTKGExtractionSpark}
import ai.scads.odibel.main.DBpediaTKG.{FlatRepartitioner, TemporalExtraction, WikidumpRevisionSplit}
import ai.scads.odibel.utils.HDFSUtil
import picocli.CommandLine.{Command, Option}

import java.io.{File, OutputStreamWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Callable
import scala.io.Source
import scala.jdk.CollectionConverters._

object DBpediaTKG {

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
      System.err.println(s"in: ${in} out: ${out}")

      val extraction = new DBpediaTKGExtraction

      if (in == "-") {
        val inSource = Source.stdin
        val ters: Iterator[TemporalExtractionResult] = extraction.processStream(inSource.getLines(), diefEndpoints.asScala.head)

        format match {
          case "csv" =>
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
          case "ngraph" =>
            val os = System.out
            ters.flatMap(SerUtil.buildQuads).grouped(100).foreach(rows => {
              os.write((rows.mkString("\n")+"\n").getBytes(StandardCharsets.UTF_8))
              os.flush()
            })
          case value =>
            System.err.println(s"format ${value} not implemented")
        }

      } else {
        val urls = parseEndpointPatternList(diefEndpoints)

        val hdfs = new HDFSUtil(out)
        hdfs.createDir(out)
        hdfs.getFs.close()

        System.err.println(urls)

        val extraction = new DBpediaTKGExtraction
        extraction.process(in, out, urls)
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
