package ai.scads.odibel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// hdfs://athena1.informatik.intern.uni-leipzig.de:9000/user/hofer/wdc/2023/jsonld.bz2/dpef.html-embedded-jsonld.nq-00000.gz.bz2
object RdfVoidSpark extends App {

  private val schema = StructType(Array(
    StructField("s", StringType, nullable = true),
    StructField("p", StringType, nullable = true),
    StructField("o", StringType, nullable = true)
  ))

  def parseNt(str: String): (String, String, String) = {
    val split = str.split(" ")
    val s = split(0)
    val p = split(1)
    val o = split(2)
    (s, p, o)
  }

  val spark = SparkSession.builder().getOrCreate()

  val rdd = spark.sparkContext.textFile(args(0))

  val nt_df = spark.createDataFrame(rdd.map(parseNt)).toDF("s", "p", "o")

  val total_triples = nt_df.count()

  val info =
    s"""#####
       |
       |$total_triples
       |
       |#####""".stripMargin
  println(info)
}
