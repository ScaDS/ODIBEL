package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.tansform.ToNQuads
import org.scalatest.funsuite.AnyFunSuite

class UtilTest extends AnyFunSuite {

  test("date timestamp conversion") {

    println(ToNQuads.formatDate(1724399176))
  }
}
