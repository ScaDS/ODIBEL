package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.transform.SerUtil
import org.apache.spark.sql.Row

import java.io.{BufferedReader, FileReader}

object CSVToRDFReification {

  private val PREFIXES =
    """@prefix dbo: <http://dbpedia.org/ontology/> .
      |@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
      |@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      |@prefix rel: <http://example.org/relation/> .
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: <inputPath> <outputPath> [--no-spark]")
      System.exit(1)
    }

    val useSpark = !args.contains("--no-spark")

    if (useSpark) {
      runWithSpark(args(0), args(1))
    } else {
      runWithoutSpark(args(0), args(1))
    }


  }

  private def runWithSpark(inputPath: String, outputPath: String): Unit = {
    println("Starting Spark session...")
    val spark = org.apache.spark.sql.SparkSession.builder()
      .appName("CSV to RDF Reification (Spark Mode)")
      .master("local[*]")
      .getOrCreate()

    try {
      val lines = spark.read.textFile(inputPath)
      val rdfData = lines.rdd
        .map(line => convertRowToRDF(line))
        .filter(_.nonEmpty)

      spark.sparkContext.parallelize(Seq(PREFIXES)).saveAsTextFile(outputPath + "_prefixes")
      rdfData.saveAsTextFile(outputPath + "_data")
      println(s"Spark output saved to $outputPath (merge with hadoop fs -cat)")

    } finally {
      spark.stop()
    }
  }


  private def runWithoutSpark(inputPath: String, outputPath: String): Unit = {
    println("Using Java Streams (no Spark)...")
    val writer = new java.io.PrintWriter(outputPath)
    writer.write(PREFIXES)

    try {
      val reader = new BufferedReader(new FileReader(inputPath))
      var line: String = reader.readLine() // Skip header line
      var count = 0

      while ({line = reader.readLine(); line != null}) {
        writer.write(convertRowToRDF(line))
        count += 1
        if (count % 1000 == 0) println(s"Processed $count lines")
      }

      println(s"Finished. Total lines: $count")
    } finally {
      writer.close()
    }
  }

  private def convertRowToRDF(line:String): String = {
    try{

      val cleanedLine = line.replace("\\\"\"", "")

      SerUtil.readCsvLine(cleanedLine) match {
        case Some(SerUtil.RDFTriple(head, rel, literal, langTagOpt, tStart, tEnd, rStart, rEnd)) =>


          val objectPart = langTagOpt match {
            case Some(tag) if tag.nonEmpty => s""""$literal"$tag"""
            case _ if isUri(literal)       => s"<$literal>"
            case _                         => s""""$literal""""
          }

          /*
          s"""_:b${System.nanoTime()} a rdf:Statement ;
             |    rdf:subject <$head> ;
             |    rdf:predicate <$rel> ;
             |    rdf:object $objectPart ;
             |    rel:tStart "$tStart"^^xsd:dateTime ;
             |    rel:tEnd "$tEnd"^^xsd:dateTime ;
             |    rel:rStart "$rStart"^^xsd:long ;
             |    rel:rEnd "$rEnd"^^xsd:long .
             |""".stripMargin
           */

          val b_node = s"_:b${System.nanoTime()}"

          s"""$b_node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement> .
             |$b_node <http://www.w3.org/1999/02/22-rdf-syntax-ns#subject> <$head> .
             |$b_node <http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate> <$rel> .
             |$b_node <http://www.w3.org/1999/02/22-rdf-syntax-ns#object> $objectPart .
             |$b_node <http://example.org/relation/tStart> "$tStart"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
             |$b_node <http://example.org/relation/tEnd> "$tEnd"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
             |$b_node <http://example.org/relation/rStart> "$rStart"^^<http://www.w3.org/2001/XMLSchema#long> .
             |$b_node <http://example.org/relation/rEnd> "$rEnd"^^<http://www.w3.org/2001/XMLSchema#long> .
             |""".stripMargin

        case None => ""
      }

    } catch {
      case _: NumberFormatException =>
        println("Error reading row - SKIP")
        ""
      case e: Exception =>
        println(s"Unexpected error: ${e.getMessage}")
        ""
    }
  }

  def isUri(s: String): Boolean = {
    val uriRegex = "^[a-zA-Z][a-zA-Z0-9+.-]*:.*".r  // begins with http://, https://, ftp:, urn:, ...
    uriRegex.pattern.matcher(s).matches
  }

}