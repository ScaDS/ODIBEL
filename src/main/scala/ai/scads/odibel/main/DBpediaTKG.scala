package ai.scads.odibel.main

import ai.scads.odibel.datasets.wikitext.{DBpediaTKGExtraction, FlatPageRevisionPartitioner, WikiDumpFlatter}
import ai.scads.odibel.main.DBpediaTKG.{FlatRepartitioner, TemporalExtraction, WikidumpRevisionSplit}
import ai.scads.odibel.utils.HDFSUtil
import picocli.CommandLine.{Command, Option}

import java.io.File
import java.util.concurrent.Callable

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

  @Command(name = "extract")
  class TemporalExtraction extends Callable[Int] {

    @Option(names = Array("-i"))
    var in: String = _

    @Option(names = Array("-o"))
    var out: String = _

    @Option(names = Array("-e"), split = ",")
    var diefEndpoints: java.util.ArrayList[String] = _

    override def call(): Int = {
      System.err.println(s"in: ${in} out: ${out}")

      val portRegex = ":(\\d+)-(\\d+)".r


      val urls = diefEndpoints.toArray.flatMap({
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

      val hdfs = new HDFSUtil(out)
      hdfs.createDir(out)
      hdfs.getFs.close()

      System.err.println(urls)

      val extraction = new DBpediaTKGExtraction
      extraction.run(in, out, urls)
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
