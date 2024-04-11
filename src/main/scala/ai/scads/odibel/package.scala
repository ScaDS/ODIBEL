import org.apache.spark.SparkConf

package object odibel {

  val availHyperThreads: Int = Runtime.getRuntime.availableProcessors()

  lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Odibel")
    sparkConf.setMaster(s"local[$availHyperThreads]")
  }

}
