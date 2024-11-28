package ai.scads.odibel.sample

import ai.scads.odibel.datasets.wikitext.RCDiefServer
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.io.Source

class Testi extends AnyFunSuite {

  import scala.concurrent.duration._

  class ThroughputMonitor {
    private var eventCount = 0L
    private val startTime = System.nanoTime()

    def mark(): Unit = this.synchronized {
      eventCount += 1
    }

    def getThroughput: Double = this.synchronized {
      val elapsedNanos = System.nanoTime() - startTime
      val elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(elapsedNanos)
      if (elapsedSeconds > 0) eventCount.toDouble / elapsedSeconds
      else 0.0
    }
  }


  test("dief server") {

    val cnt = new AtomicInteger(0)
    lazy val tm = new ThroughputMonitor
    def processCnt(): Unit = {
      val c = cnt.incrementAndGet()
      tm.mark()
      if(c % 100 == 0) println(s"e/s ${tm.getThroughput}")
    }

    val source = Source.fromFile(new File("/home/marvin/workspace/code/ODIBEL/tmp-tkge-stuff/Leipzig"))
    val xml = source.getLines().mkString("\n")
    source.close()

    val threads = 0 until 4 map {
      idx =>
      new Thread(new Runnable {
        override def run(): Unit = {
          0 until 1000 foreach {
            p =>
            val rc = new RCDiefServer(s"http://localhost:950${idx}/server/extraction/en/")
            rc.extract(xml)
            processCnt()
          }
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())
  }
}
