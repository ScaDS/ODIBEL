package ai.scads.odibel.datasets.wikitext.data

/**
 * Main output element
 * @param head
 * @param rel
 * @param tail
 * @param rFrom
 * @param rUntil
 * @param tFrom
 * @param tUntil
 */
case class TemporalExtractionResult(head: String, rel: String, tail: String, rFrom: String, rUntil: String, tFrom: Long, tUntil: Long)
