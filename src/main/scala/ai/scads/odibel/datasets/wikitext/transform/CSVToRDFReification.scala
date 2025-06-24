package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFReification extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>
        val bnode = s"_:b${System.nanoTime()}"

        s"""$bnode <${prefixes("rdf")}type> <${prefixes("rdf")}Statement> .
           |$bnode <${prefixes("rdf")}subject> <$head> .
           |$bnode <${prefixes("rdf")}predicate> <$rel> .
           |$bnode <${prefixes("rdf")}object> $obj .
           |$bnode <${prefixes("rel")}tStart> "$tStart"^^<${prefixes("xsd")}dateTime> .
           |$bnode <${prefixes("rel")}tEnd> "$tEnd"^^<${prefixes("xsd")}dateTime> .
           |$bnode <${prefixes("rel")}rStart> "$rStart"^^<${prefixes("xsd")}dateTime> .
           |$bnode <${prefixes("rel")}rEnd> "$rEnd"^^<${prefixes("xsd")}dateTime> .
           |""".stripMargin

      case None => ""
    }
  }
}
