package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFReification extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>
        s"""_:b${System.nanoTime()} a rdf:Statement ;
           |    rdf:subject <$head> ;
           |    rdf:predicate <$rel> ;
           |    rdf:object $obj ;
           |    rel:tStart "$tStart"^^xsd:dateTime ;
           |    rel:tEnd "$tEnd"^^xsd:dateTime ;
           |    rel:rStart "$rStart"^^xsd:dateTime ;
           |    rel:rEnd "$rEnd"^^xsd:dateTime .
           |""".stripMargin
      case None => ""
    }
  }
}
