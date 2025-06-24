package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFNamedGraphs extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>
        s"""graph:${System.nanoTime()} {
           |    $head $rel $obj ;
           |        rel:tStart "$tStart"^^xsd:dateTime ;
           |        rel:tEnd "$tEnd"^^xsd:dateTime ;
           |        rel:rStart "$rStart"^^xsd:dateTime ;
           |        rel:rEnd "$rEnd"^^xsd:dateTime .
           |}
           |""".stripMargin
      case None => ""
    }
  }
}
