package ai.scads.odibel.datasets.wikitext.transform

object CSVToRDFNamedGraphs extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>

        val graph = s"<${prefixes("graph")}${System.nanoTime()}>"

        s"""<${head}> <${rel}> ${obj} ${graph} .
           |<${head}> <${prefixes("rel")}tStart> "$tStart"^^<${prefixes("xsd")}dateTime> ${graph} .
           |<${head}> <${prefixes("rel")}tEnd> "$tEnd"^^<${prefixes("xsd")}dateTime> ${graph} .
           |<${head}> <${prefixes("rel")}rStart> "$rStart"^^<${prefixes("xsd")}dateTime> ${graph} .
           |<${head}> <${prefixes("rel")}rEnd> "$rEnd"^^<${prefixes("xsd")}dateTime> ${graph} .
           |""".stripMargin
      case None => ""
    }
  }
}
