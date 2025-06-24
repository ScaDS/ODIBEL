package ai.scads.odibel.datasets.wikitext.transform

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}
import java.nio.file.{Files, Paths}

object CSVToRDFNamedGraphs {

  // RDF Prefix Definitions
  private val PREFIXES =
    """@prefix dbo: <http://dbpedia.org/ontology/> .
      |@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
      |@prefix graph: <http://example.org/graph/> .
      |@prefix rel: <http://example.org/relation/> .
      |
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <inputCSVPath> <outputRDFPath>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    // Check if input file exists
    if (!Files.exists(Paths.get(inputPath))) {
      System.err.println(s"Input file not found: $inputPath")
      System.exit(1)
    }

    // Process CSV line by line
    try {
      val reader = new BufferedReader(new FileReader(inputPath))
      val writer = new BufferedWriter(new FileWriter(outputPath))

      // Write prefixes first
      writer.write(PREFIXES)

      var lineCount = 0
      var currentLine: String = reader.readLine() // Skip header line

      while ({currentLine = reader.readLine(); currentLine != null}) {
        lineCount += 1
        val graph = convertLineToRDF(currentLine, lineCount)
        writer.write(graph)
        writer.newLine()
      }

      reader.close()
      writer.close()
      println(s"Successfully converted $lineCount lines to RDF Named Graphs.")
    } catch {
      case e: Exception =>
        System.err.println(s"Error processing file: ${e.getMessage}")
        System.exit(1)
    }
  }

  private def convertLineToRDF(line: String, graphId: Int): String = {
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1) // Split CSV, handling quoted values

    if (fields.length != 7) {
      System.err.println(s"Skipping malformed line: $line")
      return ""
    }

    val Array(head, rel, tail, tStart, tEnd, rStart, rEnd) = fields.map(_.trim)

    // Clean tail value (remove quotes and ^^xsd:integer if present)
    val cleanTail = tail.replaceAll("^\"|\"$", "").replace("^<xsd:integer>", "")

    s"""graph:population$graphId {
       |    $head $rel "$cleanTail"^^xsd:integer;
       |        rel:tStart "$tStart"^^xsd:date;
       |        rel:tEnd "$tEnd"^^xsd:date;
       |        rel:rStart $rStart;
       |        rel:rEnd $rEnd.
       |}
       |""".stripMargin
  }
}