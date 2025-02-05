package ai.scads.odibel.datasets.wikitext.data

/**
 * Main Input Element
 * @param pagexml the inner xml elements of the "page" element without revisions
 * @param revisionxml the inner xml elements of the "revision" element
 */
case class PageRevision(pId: Long, rId: Long, rTimestamp: Long, pagexml: String, revisionxml: String, ns: Option[Int] = None)


