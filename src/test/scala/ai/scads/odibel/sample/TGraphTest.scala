package ai.scads.odibel.sample

import ai.scads.odibel.datasets.wikitext.TemporalWindowBuilder
import ai.scads.odibel.graph.Triple
import org.scalatest.funsuite.AnyFunSuite

class TGraphTest extends AnyFunSuite {

  /*
  T1 Graph:
  e1 p1 o1 .
  e1 p2 o2 .
  e1 p2 o3 .
   */

  /*
  T2 Graph
  e1 p1 o1 .
  e1 p2 o21 .
  e1 p2 o3 .
   */

  val t1g = List(
    Triple("e1", "p1", "o1"),
    Triple("e1", "p2", "o2"),
    Triple("e1", "p2", "o3")
  )


  val t2g = List(
    Triple("e1", "p1", "o1"),
    Triple("e1", "p2", "o21"),
    Triple("e1", "p2", "o3")
  )

  test("building") {

    val tb = new TemporalWindowBuilder

    tb.addGraphVersion(t1g.map(t => s"<${t.s}> <${t.p}> <${t.o}> ."),0)()
//    tb.buildQuads().foreach(println)
    println("---")
    tb.addGraphVersion(t2g.map(t => s"<${t.s}> <${t.p}> <${t.o}> ."),1)()
    tb.buildEntries().foreach(println)
  }

  test("set test") {

    val s1 = Set(t1g.map(t => s"<${t.s}> <${t.p}> <${t.o}> ."): _*)
    val s2 = Set(t2g.map(t => s"<${t.s}> <${t.p}> <${t.o}> ."): _*)

  }
}
