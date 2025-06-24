package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFStar extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>
        s"""<< <$head> <$rel> $obj >> <${prefixes("rel")}tStart> "$tStart"^^<${prefixes("xsd")}dateTime> .
           |<< <$head> <$rel> $obj >> <${prefixes("rel")}tEnd> "$tEnd"^^<${prefixes("xsd")}dateTime> .
           |<< <$head> <$rel> $obj >> <${prefixes("rel")}rStart> "$rStart"^^<${prefixes("xsd")}dateTime> .
           |<< <$head> <$rel> $obj >> <${prefixes("rel")}rEnd> "$rEnd"^^<${prefixes("xsd")}dateTime> .
           |""".stripMargin
      case None => ""
    }
  }
}
