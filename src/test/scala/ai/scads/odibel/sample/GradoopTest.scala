package ai.scads.odibel.sample

import ai.scads.odibel.graph.TemporalTriple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class GradoopTest extends AnyFunSuite {


  /* Input Example
  s1;p1;o11;vs;ve
  s1;p1;o12;vs;ve
  s1;p2;o21;vs;ve
  s1;p3;o31;vs;ve
  s2;p2;o41;vs;ve
  s2;p3;o51;vs;ve
   */

  /* Output Template
  file:///out/graph.csv
  guuid1;;;(ts,te);(vs,ve)

  file:///out/metadata.csv
  v;ELabel1;VPropertyKey1:VPropertyType1,VPropertyKey2:VPropertyType2
  e;ELabel1;EPropertyKey1:EPropertyType1,EPropertyKey2:EPropertyType2,EPropertyKey3:EPropertyType3

  file:///out/vertices.csv/1
  vuuid1;guuid1;VLabel1;VPropertyValue1|VPropertyValue2;(ts,te);(vs,ve)

  file:///out/edges.csv/1
  euuid1;guuid1;ELabel1;EPropertyValue1||EPropertyValue3;(ts,te);(vs,ve)
   */
  test("Some test") {

    val inputCSV =
      """  s1;p1;o11;v1;v2
        |  s1;p1;o12;v2;vN
        |  s1;p2;"o21";v1;vN
        |  s1;p3;o31;v1;vN
        |  s2;p2;"o41";v1;vN
        |  s2;p3;o51;v1;vN
        |  s3;p4;"o61";v1;vN""".stripMargin

    val spark = SparkSession.builder().master("local[4]").getOrCreate()

    val csvRDD: RDD[TemporalTriple] = spark.sparkContext.parallelize(inputCSV.split("\n")).map({
      textRow =>
        val Array(s, p, o, vs, ve) = textRow.trim.split(";", 5)
        TemporalTriple(s, p, o, vs, ve)
    })

    val nodeCentric = csvRDD.filter(!_.o.startsWith("\"")).groupBy(_.s)
    val edgeCentric = csvRDD.filter(_.o.startsWith("\"")).groupBy(_.p)

    println("node centric")
    val verticesCSV =
      nodeCentric.collect().flatMap({
        case (nodeUri, temporalTriples) =>
          temporalTriples.groupBy(tt => tt.vs + "," + tt.ve).map({
            case (windowKey, windowedTriple) =>
              s"$nodeUri;;Node;" + windowedTriple.map(_.o).mkString("|") + s";($windowKey);($windowKey)"
          })
      })
    println(verticesCSV.mkString("\n"))


    println("edge centric")
    val edgesCSV =
      edgeCentric.collect().flatMap({
        case (edgeUri, temporalTriples) =>
          temporalTriples.groupBy(tt => tt.vs + "," + tt.ve).map({
            case (windowKey, windowedTriple) =>
              s"$edgeUri;;Link;" + windowedTriple.map(_.o).mkString("|") + s";($windowKey);($windowKey)"
          })
      })

    println(edgesCSV.mkString("\n"))

    //TODO most specific type...
    //TODO schema extraction...
    //TODO schema clustering...
  }
}
