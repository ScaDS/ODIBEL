package ai.scads.odibel.datasets.wikitext.log

import org.slf4j.LoggerFactory

// Ask Petra
// has monitor
// has prints to output log
// updates monitor?
class EventLogger(monitor: HeartbeatMonitor) {

  private val log = LoggerFactory.getLogger(classOf[EventLogger])

  def logEvent(event: Event): Unit = {
    monitor.registerEvent(event)
    log.info(event.toString)
  }
}
