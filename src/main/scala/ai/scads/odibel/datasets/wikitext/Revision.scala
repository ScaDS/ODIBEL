package ai.scads.odibel.datasets.wikitext

/**
 * Flattened Revision with page information
 * @param title title of the wikipage
 * @param pId page id
 * @param ns the wiki page namespace 0 == article page
 * @param rId revision id
 * @param timestamp timestamp
 * @param text wikitext
 */
case class Revision(title: String, pId: Long, ns: Int, rId: Long, timestamp: Long, text: String)
