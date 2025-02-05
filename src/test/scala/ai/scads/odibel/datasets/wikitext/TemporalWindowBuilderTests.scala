package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.eval.VOCAB
import org.scalatest.funsuite.AnyFunSuite

class TemporalWindowBuilderTests extends AnyFunSuite {

  test("unwrap triples") {

    val ntDoc =
      s"""<http://dbpedia.org/resource/Leipzig> <${VOCAB.RDFType}> <${VOCAB.DBO}Place> .
         |<http://dbpedia.org/resource/Leipzig> <${VOCAB.DBO}populationTotal> "100" .
         |<http://dbpedia.org/resource/Leipzig> <${VOCAB.DBO}populationTotal> "100"^^<${VOCAB.XSD}integer> .""".stripMargin

    val twb = new TemporalWindowBuilder

    ntDoc.split("\n").map(twb.unwrapNTriple).foreach(println)
  }

  test("empty builder") {

  }
}
