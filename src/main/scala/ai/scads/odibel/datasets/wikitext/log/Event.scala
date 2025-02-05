package ai.scads.odibel.datasets.wikitext.log

trait Event

sealed trait PageEvent extends Event
case class SucceededPageEvent(pId: Long, provenance: Map[String,Any]) extends PageEvent
case class FailedPageEvent(pId: Long, error: String) extends PageEvent

sealed trait RevisionEvent extends Event
case class SucceededRevisionEvent(rId: Long, meta: Map[String,Any]) extends RevisionEvent
case class FailedRevisionEvent(rId: Long, error: String) extends RevisionEvent
case class SkippedRevisionEvent(rId: Long) extends RevisionEvent