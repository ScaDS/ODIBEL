package ai.scads.odibel.datasets.wikitext.extraction

import ai.scads.odibel.datasets.wikitext.log.{EventLogger, HeartbeatMonitor}

import java.nio.file.Paths
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ListBuffer

// Creates all Extraction Runners
class Executor(extractionJobs: List[ExtractionJob], endpoints: List[String], eventLog: EventLogger) {

  val threads = new ListBuffer[Thread]()
  val jobQueue = new LinkedBlockingQueue[ExtractionJob]()


  extractionJobs.foreach(jobQueue.put)

  endpoints.foreach({
    endpoint =>
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          while (!jobQueue.isEmpty) {
            val job = jobQueue.poll() // Retrieve and remove an element, or null if empty
            if (job != null) {
              val runner = new ExtractionRunner(job, endpoint: String, eventLog)
              runner.execute()
            }
          }
        }
      })
      threads.append(thread)
  })


  def start(): Unit = {
    threads.foreach(_.start())
  }

  def waitForCompletion(): Unit = {
    threads.foreach(thread => thread.join())
  }
}
