package ai.scads.odibel.datasets.wikitext

case class TemporalStatement(head: String, relation: String, tail: String, revStart: Long, revEnd: Long, timeStart: Long, timeEnd: Long)
