package ai.scads.odibel.datasets.wikitext.log

trait RevisionEvent extends Event

case class SucceededRevisionEvent(rId: Long, meta: Map[String,Any]) extends RevisionEvent

case class FailedRevisionEvent(rId: Long, error: String) extends RevisionEvent

case class SkippedRevisionEvent(rId: Long) extends RevisionEvent