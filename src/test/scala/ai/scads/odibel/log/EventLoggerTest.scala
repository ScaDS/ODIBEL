package ai.scads.odibel.log

import ai.scads.odibel.datasets.wikitext.log.{EventLogger, HeartbeatMonitor, SucceededPageEvent}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class EventLoggerTest extends AnyFunSuite {

  test("log event") {

    val hm = new HeartbeatMonitor()
    val el = new EventLogger(hm)

    el.logEvent(SucceededPageEvent(0,Map()))
  }

  test("timestamp generation") {
    val startTime2 = System.nanoTime

    for (i <- 0 until 1000000) {
      System.currentTimeMillis()
    }

    val endTime2 = System.nanoTime

    System.out.println("Total time currentTimeMillis: " + (endTime2 - startTime2))

    val startTime = System.nanoTime

    for (i <- 0 until 1000000) {
      System.nanoTime
    }

    val endTime = System.nanoTime



    System.out.println("Total time nanoTime: " + (endTime - startTime))


  }
}
