package ai.scads.odibel.utils

import java.util.concurrent.TimeUnit

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

  def markAndGet(): Long = {
    eventCount +=1
    eventCount
  }
//  def markAndPrint(minDelayNano: Long): Unit = this.synchronized {
//    eventCount += 1
//
//  }
}