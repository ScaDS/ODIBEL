package ai.scads.odibel.datasets.wikitext

import ai.scads.odibel.datasets.wikitext.DBpediaTKGExtraction.processPageRevisionIterator
import ai.scads.odibel.datasets.wikitext.io.IOUtil
import ai.scads.odibel.datasets.wikitext.utils.SparkSessionUtil
import org.apache.spark.sql.Row

import java.time.Instant

// TODO check and write down todos

/**
 * Version that is using SPARK for load distribution
 */
object DBpediaTKGExtractionSpark {

  private val session = SparkSessionUtil.spark
  private val sparkcontext = session.sparkContext
  private val sparksql = session.sqlContext

  import sparksql.implicits._

  def call(inPaths: List[String], sinkPath: String, diefEndpoints: List[String]): Int = {

    val poolSize = diefEndpoints.length

    val assignedDumpsPathString = inPaths.zipWithIndex.map({
      case (filePath, idx) =>
        (filePath, diefEndpoints(idx % poolSize))
    })

    val dumpsRDD = sparksql.createDataset(assignedDumpsPathString).toDF("dumpPath", "dief")

    dumpsRDD.repartition(poolSize, $"dief").mapPartitions({
      rows: Iterator[Row] =>
        val rowList = rows.toList
        if (rowList.nonEmpty) {
          val diefEndpoint = rowList.head.getAs[String]("dief")
          val orderedPageRevisionIterator = IOUtil.readFilesSequentially(rowList.map(_.getAs[String]("dumpPath")))
          val (ters, count) = processPageRevisionIterator(orderedPageRevisionIterator, diefEndpoint)
          ters
        } else {
          Iterator.empty
        }
    }).write.mode("overwrite").parquet(sinkPath)

    sparkcontext.stop()
    0
  }

  def dateToStamp(dateTime: String): Long = {
    Instant.parse(dateTime).getEpochSecond
  }
}
