package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFNamedGraphs extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, literal, langTagOpt, tStart, tEnd, rStart, rEnd)) =>
        val langTagStr = langTagOpt.getOrElse("")
        s"""graph:${System.nanoTime()} {
           |    $head $rel "$literal"$langTagStr ;
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
