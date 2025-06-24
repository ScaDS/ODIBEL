package ai.scads.odibel.datasets.wikitext.transform

object TKGModelMgr {

  sealed trait TKGModel { val format: String }
  case object RDFNamedGraph extends TKGModel { val format = "nquads" }
  case object RDFProperty extends TKGModel { val format = "property" }

  def serializeAs(): Unit = {

  }
}

trait CSVToRDF {

  val prefixes: Map[String, String] = Map(
    "rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rel" -> "http://example.org/relation/",
    "graph" -> "http://example.org/graph/",
    "xsd" -> "http://www.w3.org/2001/XMLSchema#"
  )

  def convertRowToRDF(line: String): String

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: <inputPath> <outputPath> [--no-spark]")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val useSpark = !args.contains("--no-spark")

    if (useSpark) {
      runWithSpark(inputPath, outputPath, convertRowToRDF)
    } else {
      runWithoutSpark(inputPath, outputPath, convertRowToRDF)
    }
  }

  private def runWithSpark(
                            inputPath: String,
                            outputPath: String,
                            convert: String => String
                          ): Unit = {
    println("Starting Spark session...")
    val spark = org.apache.spark.sql.SparkSession.builder()
      .appName("CSV to RDF")
      .master("local[*]")
      .getOrCreate()

    try {
      val df = spark.read.option("header", "true").csv(inputPath)
      val rdfData = df.rdd.mapPartitions { rows =>
        rows.map(row => convert(row.mkString(",")))
      }
      rdfData.saveAsTextFile(outputPath + "_data")
      println(s"Spark output saved to $outputPath (merge with hadoop fs -cat)")
    } finally {
      spark.stop()
    }
  }

  private def runWithoutSpark(
                               inputPath: String,
                               outputPath: String,
                               convert: String => String
                             ): Unit = {
    val source = scala.io.Source.fromFile(inputPath)
    val lines = try source.getLines().drop(1).toSeq finally source.close()

    val converted = lines.map(convert)
    val pw = new java.io.PrintWriter(outputPath)
    try {
      converted.filter(_.nonEmpty).foreach(pw.println)
    } finally pw.close()
  }

}

object CSVToRDFNamedGraphs extends CSVToRDF {
  override def convertRowToRDF(line: String): String = {
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, obj, tStart, tEnd, rStart, rEnd)) =>

        val graph = s"<${prefixes("graph")}${System.nanoTime()}>"

        s"""<$head> <$rel> $obj $graph .
           |<$head> <${prefixes("rel")}tStart> "$tStart"^^<${prefixes("xsd")}dateTime> $graph .
           |<$head> <${prefixes("rel")}tEnd> "$tEnd"^^<${prefixes("xsd")}dateTime> $graph .
           |<$head> <${prefixes("rel")}rStart> "$rStart"^^<${prefixes("xsd")}dateTime> $graph .
           |<$head> <${prefixes("rel")}rEnd> "$rEnd"^^<${prefixes("xsd")}dateTime> $graph .
           |""".stripMargin
      case None => ""
    }
  }
}

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
