package ai.scads.odibel.datasets.wikitext

import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

//@Ignore
class ExtractionTest extends AnyFunSuite {

  test("extract leipzig") {

//    val path = "/home/marvin/paper/dbpedia-tkg/testdata/dump_with_leipzig/page_Leipzig.xml"
    val path = "/home/marvin/paper/dbpedia-tkg/testdata/dump_with_leipzig/page_Leipzig_last50.xml"

    val diefURL = "http://localhost:59001/server/extraction/en"

    val extractor = DBpediaTKGExtractionSpark
    extractor.call(List(path), "/home/marvin/paper/dbpedia-tkg/out", List(diefURL))
  }
}
