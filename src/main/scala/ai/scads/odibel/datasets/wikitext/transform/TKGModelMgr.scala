package ai.scads.odibel.datasets.wikitext.transform

object TKGModelMgr {

  sealed trait TKGModel { val format: String }
  case object RDFNamedGraph extends TKGModel { val format = "nquads" }
  case object RDFProperty extends TKGModel { val format = "property" }

  def serializeAs(): Unit = {

  }
}
