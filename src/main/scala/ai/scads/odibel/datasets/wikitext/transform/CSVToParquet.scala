package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil

object CSVToParquet extends App {

  if (args.length < 2) {
    System.err.println("Usage: CSVToParquet <input_csv> <output_parquet>")
    System.exit(1)
  }

  val inputPath = args(0)
  val outputPath = args(1)

  private val sql = SparkSessionUtil.sql

  sql.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("unescapedQuoteHandling", "BACK_TO_DELIMITER")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(inputPath)
    .write
    .csv(outputPath)

}

