package ai.scads.odibel.graph

import org.apache.spark.sql.{DataFrame, SparkSession}

class RDF() {

//  def join(df1:DataFrame, df2:DataFrame, id1: String, id2: String)(implicit spark: SparkSession): DataFrame = {

}


object RDF {

  case class Triple(s: String, p: String, o: String)

  def main(args: Array[String]): Unit = {

    val csv1Path = "/media/marvin/tmp/workspace/data/imdb/full/title.basics.tsv.bz2"
    val csv2Path = "/media/marvin/tmp/workspace/data/imdb/full/title.principals.tsv.bz2"

    implicit val spark: SparkSession =
      SparkSession.builder().master("local[*]").appName("ODIBEL").getOrCreate()

    // read csv file with headers option into dataframe
    val df1 = prefixedColumns(spark.read.option("header", true).option("delimiter","\t").csv(csv1Path),"c1")
    df1.show(5, false)

    val df2 =  prefixedColumns(spark.read.option("header", true).option("delimiter","\t").csv(csv1Path),"c2")

    val id1 = "c1_tconst"
    val id2 = "c2_tconst"

//    val rdf = new RDF()

    val join = df1.join(df2, df1(id1) === df2(id2), "outer")
    join.show(5, false)
  }


  def prefixedColumns(dataframe: DataFrame, prefix: String)(implicit spark: SparkSession): DataFrame = {

    val old_columns = dataframe.columns
    val new_columns: Map[String, String] = old_columns.zip(old_columns.map(prefix  +"_"+_)).toMap[String, String]
    dataframe.withColumnsRenamed(new_columns)
  }
}
