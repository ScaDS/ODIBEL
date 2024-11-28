package ai.scads.odibel.graph

trait Graph {

  val nodes: List[Node]
  val edges: List[Edge]

  trait Node {
    val id: String
    val label: String
    val properties: Map[String, String]
  }

  trait Edge {
    val from: String
    val to: String
    val label: String
    val properties: Map[String, String]
  }
}
