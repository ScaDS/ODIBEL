package ai.scads.odibel.datasets.wikitext.data

/**
 * Main output element
 * @param head
 * @param rel
 * @param tail
 * @param rStart
 * @param rEnd
 * @param tStart
 * @param tEnd
 */
case class TemporalExtractionResult(head: String, rel: String, tail: String, rStart: String, rEnd: String, tStart: java.lang.Long, tEnd: java.lang.Long)
