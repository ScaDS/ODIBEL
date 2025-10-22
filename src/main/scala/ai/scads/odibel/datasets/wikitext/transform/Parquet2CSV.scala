package ai.scads.odibel.datasets.wikitext.transform

import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil

object Parquet2CSV extends App {

  val spark = SparkSessionUtil

  spark.sql.read.parquet("/home/marvin/paper/dbpedia-tkg/out").write.option("header","true").csv("/home/marvin/paper/dbpedia-tkg/out.csv")

}
