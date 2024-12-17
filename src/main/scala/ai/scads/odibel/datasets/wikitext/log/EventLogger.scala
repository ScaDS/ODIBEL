package ai.scads.odibel.datasets.wikitext.log

import org.slf4j.LoggerFactory

// Ask Petra
class EventLogger(monitor: HeartbeatMonitor) {

  val log = LoggerFactory.getLogger(classOf[EventLogger])

  def logEvent(event: Event): Unit = {
    monitor.registerEvent(event)
    log.info(event.toString)
  }

  // has monitor
  // has prints to output log
  // updates monitor?
}
