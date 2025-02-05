package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.tansform.{SerUtil, ToNQuads}
import org.scalatest.funsuite.AnyFunSuite

class UtilTest extends AnyFunSuite {

  test("date timestamp conversion") {

    println(SerUtil.formatDate(1724399176))
  }
}
