package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.transform.CSVToRDFReification.{PREFIXES, convertRowToRDF}
import ai.scads.odibel.datasets.wikitext.transform.SerUtil
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import org.apache.spark.sql.Row

import java.io.{BufferedReader, FileReader}

object CSVToRDFNamedGraphs {

  private val PREFIXES =
    """@prefix dbo: <http://dbpedia.org/ontology/> .
      |@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
      |@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      |@prefix rel: <http://example.org/relation/> .
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: <inputPath> <outputPath>")
      System.exit(1)
    }


    runWithSpark(args(0), args(1))



  }


  private def runWithSpark(inputPath: String, outputPath: String): Unit = {

    val sql = SparkSessionUtil.sql

    val lines = sql.read.textFile(inputPath)
    val rdfData = lines.rdd
      .zipWithIndex()
      .map { case (line, idx) => convertRowToRDF(line, idx.toInt) }
      .filter(_.nonEmpty)

    sql.sparkContext.parallelize(Seq(PREFIXES)).saveAsTextFile(outputPath + "_prefixes")
    rdfData.saveAsTextFile(outputPath + "_data")
    println(s"Spark output saved to $outputPath (merge with hadoop fs -cat)")


  }

  private def run(inputPath: String, outputPath: String): Unit = {
    println("Using Java Streams (no Spark)...")
    val writer = new java.io.PrintWriter(outputPath)

    val spark = SparkSessionUtil
    writer.write(PREFIXES)

    try {
      val reader = new BufferedReader(new FileReader(inputPath))
      var line: String = reader.readLine() // Skip header line
      var count = 0

      while ({line = reader.readLine(); line != null}) {
        writer.write(convertRowToRDF(line, count))
        count += 1
        if (count % 1000 == 0) println(s"Processed $count lines")
      }

      println(s"Finished. Total lines: $count")
    } finally {
      writer.close()
    }
  }

  private def convertRowToRDF(line:String, count: Int): String = {
    try{

      val cleanedLine = line.replace("\\\"\"", "")

      SerUtil.readCsvLine(cleanedLine) match {
        case Some(SerUtil.RDFTriple(head, rel, literal, langTagOpt, tStart, tEnd, rStart, rEnd)) =>


          val objectPart = langTagOpt match {
            case Some(tag) if tag.nonEmpty => s""""$literal"$tag"""
            case _ if isUri(literal)       => s"<$literal>"
            case _                         => s""""$literal""""
          }

          s"""graph:population$count {
             |    $head $rel $objectPart;
             |        rel:tStart "$tStart"^^xsd:date;
             |        rel:tEnd "$tEnd"^^xsd:date;
             |        rel:rStart $rStart;
             |        rel:rEnd $rEnd.
             |}
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