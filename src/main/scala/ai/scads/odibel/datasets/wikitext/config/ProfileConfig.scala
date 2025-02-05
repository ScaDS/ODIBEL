package ai.scads.odibel.datasets.wikitext.config

// Contains all params used to the TKG extraction
// TODO has to be finished immediately
object ProfileConfig {

  /**
   * cron job like pattern to select revisions to extract
   */
  val timestampRevisionFilter: String = "* * * * *"

  /**
   * applied after timestampRevisionFilter
   * if zero select every revision
   */
  val nSkipRevisionFilter: Int = 0

  val wikiNamespaceFilter: Set[Int] = Set(0,14)
}