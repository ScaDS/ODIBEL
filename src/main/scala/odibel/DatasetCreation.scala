package odibel

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD

import scala.util.Random

object DatasetCreation {
  val sparkContext = new SparkContext(new SparkConf().setMaster("local[1]").setAppName("ntriple-reader"))
  val inputDataset = "src/main/resources/data/demo-graph.nt.bz2"
  sparkContext.setLogLevel("ERROR")
  def main(args: Array[String]) {
    val graph = readNTriples(sparkContext, inputDataset)
    //graph.triplets.collect.foreach(t => println(t.srcId, t.srcAttr, t.attr, t.dstAttr))
    println("Degree Vector: ")
    degreeVector(graph).foreach(x => println(x))

    sparkContext.stop
  }

  def degreeVector(graph: Graph[String, String]): Vector[Long] = {
    //println("Graph outdegrees: ")
    //graph.outDegrees.foreach(t => println(t._1, t._2))

    // TODO: write separate implicit for max
    val maxOutDegree = graph.outDegrees.max()((x, y) => x._2 - y._2)._2
    val array = new Array[Long](maxOutDegree)
    for (v <- graph.outDegrees.collect()) {
      array.update(v._2-1, array(v._2-1) + 1)
    }

    array.toVector
  }

  def parseNTriple(triple: String): Array[String] = {
    val splitTriple = triple.split(" ")
    val s = splitTriple(0)
    val p = splitTriple(1)
    val o = splitTriple(2)
   // println("SPLIT TRIPLE: " + s + " ; " + p + " ; " +  o)
    Array(s, p, o)
  }

  def makeEdge(triple: Array[String]): (Array[(VertexId, String)], Edge[String]) = {
    val subjectId = triple(0).hashCode.toLong
    val objectId = triple(2).hashCode.toLong
    (Array((subjectId, triple(0)), (objectId, triple(2))), Edge(subjectId, objectId, triple(1)))
  }

  def readNTriples(sc: SparkContext, filename: String): Graph[String, String] = {
    val r = sc.textFile(filename).map(parseNTriple)
    val z = r.map(makeEdge).collect()
    val vertices = sc.makeRDD(z.flatMap(x => x._1))
    val edges = sc.makeRDD(z.map(x => x._2))
    Graph(vertices, edges)
  }

}
