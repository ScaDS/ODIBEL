package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFReification extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, literal, langTagOpt, tStart, tEnd, rStart, rEnd)) =>
        val langTagStr = langTagOpt.getOrElse("")
        s"""_:b${System.nanoTime()} a rdf:Statement ;
           |    rdf:subject <$head> ;
           |    rdf:predicate <$rel> ;
           |    rdf:object "$literal"$langTagStr ;
           |    rel:tStart "$tStart"^^xsd:dateTime ;
           |    rel:tEnd "$tEnd"^^xsd:dateTime ;
           |    rel:rStart "$rStart"^^xsd:dateTime ;
           |    rel:rEnd "$rEnd"^^xsd:dateTime .
           |""".stripMargin
      case None => ""
    }
  }
}
