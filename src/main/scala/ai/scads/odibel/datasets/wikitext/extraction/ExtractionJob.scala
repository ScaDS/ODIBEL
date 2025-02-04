package ai.scads.odibel.datasets.wikitext.extraction

/**
 * Encapsulates an extraction job
 * @param sourceUri source location of dump input file
 * @param sinkUri sink location of extracted results
 */
case class ExtractionJob(sourceUri: String, sinkUri: String)
