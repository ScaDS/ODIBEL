package ai.scads.odibel.datasets.wikitext

import upickle.legacy.{ReadWriter, macroRW}

/**
 *
 * @param pagexml the inner xml elements of the "page" element without revisions
 * @param revisionxml the inner xml elements of the "revision" element
 */
case class FlatRawPageRevision(pagexml: String, revisionxml: String)


