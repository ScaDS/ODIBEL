package ai.scads.odibel.datasets.wikitext.log

import ai.scads.odibel.datasets.wikitext.extraction.ExtractionRunner
import ai.scads.odibel.utils.ThroughputMonitor

import java.util.concurrent.atomic.AtomicLong

class HeartbeatMonitor extends Runnable {

  val tm = new ThroughputMonitor

  private var runner = List[Thread]()
  def registerRunnerThreads(threads: List[Thread]): Unit = {
    runner = threads
  }

  val processedRevisions = new AtomicLong(0)
  val skippedRevisions = new AtomicLong(0)
  val failedRevisions = new AtomicLong(0)

  val processedPage = new AtomicLong(-1)
  val skippedPage = new AtomicLong(0)
  val failedPage = new AtomicLong(0)

  def registerEvent(event: Event): Unit = {

    event match {
      case pageEvent: PageEvent =>
        pageEvent match {
          case failedPE: FailedPageEvent =>
            failedPage.incrementAndGet()
          case succeededPE: SucceededPageEvent =>
            processedPage.incrementAndGet()
        }
      case revisionEvent: RevisionEvent =>
        tm.mark()
        revisionEvent match {
          case failedRE: FailedRevisionEvent =>
            //TODO
            failedRevisions.incrementAndGet()
          case succeededRE: SucceededRevisionEvent =>
            //TODO
            processedRevisions.incrementAndGet()
          case skippedRevisionEvent: SkippedRevisionEvent =>
            skippedRevisions.incrementAndGet()
        }
      case unknownEvent => //TODO
    }
  }

  def show(): Unit = {
    val sb = new StringBuilder()
    sb.append("===\n")
    sb.append(s"running ${runner.count(_.isAlive)} of ${runner.length}\n")
    sb.append(s"revision $processedRevisions $skippedRevisions $failedRevisions (${tm.getThroughput}/s)\n")
    sb.append(s"page $processedPage $skippedPage $failedPage\n")
    sb.append("===\n")

    println(sb)
  }
  override def run(): Unit = {

    while (true) {
      Thread.sleep(10000)
      show()
    }
  }
}
