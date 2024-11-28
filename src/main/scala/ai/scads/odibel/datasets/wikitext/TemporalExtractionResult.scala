package ai.scads.odibel.datasets.wikitext

case class TemporalExtractionResult(head: String, rel: String, tail: String, rFrom: String, rUntil: String, tFrom: Long, tUntil: Long)
