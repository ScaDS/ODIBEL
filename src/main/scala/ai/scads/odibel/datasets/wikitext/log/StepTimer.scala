package ai.scads.odibel.datasets.wikitext.log

import org.slf4j.LoggerFactory
import scala.collection.mutable

class StepTimer(taskName: String) {

  private val log = LoggerFactory.getLogger(classOf[StepTimer])
  private val durations = mutable.Map[String, Long]()

  def timeStep[R](stepName: String)(block: => R): R = {
    val start = System.nanoTime()
    try {
      block
    } finally {
      val duration = (System.nanoTime() - start) / 1_000_000 // in milliseconds
      durations.update(stepName, duration)
      log.info(s"$taskName - $stepName took $duration ms")
    }
  }

  def summarize(): Unit = {
    log.info(s"Summary for task $taskName:")
    durations.foreach { case (step, duration) =>
      log.info(s"  $step: $duration ms")
    }
  }
}

object StepTimer extends App {
  val log = LoggerFactory.getLogger("TaskExample")

  // Simulate a single execution of a task
  def executeTask(taskName: String): Unit = {
    val timer = new StepTimer(taskName)

    timer.timeStep("step1") {
      // Simulate step1 logic
      Thread.sleep(100) // Simulates an API call
    }

    timer.timeStep("step2") {
      // Simulate step2 logic
      Thread.sleep(200) // Simulates another API call
    }

    timer.timeStep("step3") {
      // Simulate step3 logic
      Thread.sleep(150) // Simulates final API call
    }

    timer.summarize()
  }

  // Simulate multiple task executions
  for (i <- 1 to 5) {
    log.info(s"Starting execution $i")
    executeTask(s"TaskExecution-$i")
  }
}
