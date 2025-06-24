package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.transform.SerUtil

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
      val df = spark.read.option("header", "true").csv(inputPath)
      val rdfData = df.rdd.mapPartitions { rows =>
        rows.map(row => convertRowToRDF(row.mkString(",")))
      }

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
      var line: String = null
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
    SerUtil.readCsvLine(line) match {
      case Some(SerUtil.RDFTriple(head, rel, literal, langTagOpt, tStart, tEnd, rStart, rEnd)) =>
        val langTagStr = langTagOpt.getOrElse("")
        s"""_:b${System.nanoTime()} a rdf:Statement ;
                     |    rdf:subject <$head> ;
                     |    rdf:predicate <$rel> ;
                     |    rdf:object "$literal"$langTagStr ;
                     |    rel:tStart "$tStart"^^xsd:dateTime ;
                     |    rel:tEnd "$tEnd"^^xsd:dateTime ;
                     |    rel:rStart $rStart^^xsd:dateTime ;
                     |    rel:rEnd $rEnd^^xsd:dateTime .
                     |""".stripMargin

      case None => ""
    }
  }


}