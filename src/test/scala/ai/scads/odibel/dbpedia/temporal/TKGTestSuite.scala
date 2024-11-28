package ai.scads.odibel.dbpedia.temporal

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TKGTestSuite extends AnyFunSuite {

  test("count distinct windows") {

    case class CSVRow(head: String, relation: String, tail: String, from: Long, until: Long)

    // local[*] will allocate all cpu cores
    val spark = SparkSession.builder().master({"local[*]"}).getOrCreate()

    val sparkSql = spark.sqlContext
    import sparkSql.implicits._

    val counter = sparkSql.read.csv("").as[CSVRow].map({
      row =>
        row.from.toString + row.until.toString
    }).distinct().count()

    println(counter)
  }
}
