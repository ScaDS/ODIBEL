package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.extraction.{Executor, ExtractionJob}
import ai.scads.odibel.datasets.wikitext.log.{EventLogger, HeartbeatMonitor}
import ai.scads.odibel.utils.HDFSUtil

import java.nio.file.{Path, Paths}
import scala.collection.mutable.ListBuffer

class DBpediaTKGExtraction {

  def run(source: String, sink: String, endpoints: List[String]): Unit = {

    val sourceFiles =
      if(source.startsWith("hdfs")){
        val hdfs = new HDFSUtil(source)
        val files = hdfs.listHDFSFiles(source)
        hdfs.getFs.close()
        files
      } else {
        Paths.get(source).toFile.list().toList
      }

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
  }
}
