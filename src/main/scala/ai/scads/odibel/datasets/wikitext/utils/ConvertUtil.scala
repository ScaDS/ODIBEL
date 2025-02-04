package ai.scads.odibel.datasets.wikitext.utils

/**
 * Simple tool to convert json to parquet to read faster from HDFS and into SPARK
 */
object ConvertUtil extends App {

  val sql = SparkSessionUtil.sql

  if(args.length != 3)
    System.err.println("usage -- inputPath OutputPath rePartitions")

  sql.read.json(args(0))
    .repartition(args(2).toInt)
    .write.parquet(args(1))
}
