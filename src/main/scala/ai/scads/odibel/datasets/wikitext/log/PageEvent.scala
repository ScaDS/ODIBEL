package ai.scads.odibel.datasets.wikitext.log

trait PageEvent extends Event

case class SucceededPageEvent(pId: Long, provenance: Map[String,Any]) extends PageEvent
case class FailedPageEvent(pId: Long, error: String) extends PageEvent