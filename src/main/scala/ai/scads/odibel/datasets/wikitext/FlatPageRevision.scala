package ai.scads.odibel.datasets.wikitext

/**
 *
 * @param pagexml the inner xml elements of the "page" element without revisions
 * @param revisionxml the inner xml elements of the "revision" element
 */
case class FlatPageRevision(pId: Long, rId: Long, pagexml: String, revisionxml: String)


