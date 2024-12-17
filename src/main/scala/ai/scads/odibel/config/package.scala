package ai.scads.odibel

import org.apache.spark.SparkConf

package object config {

  val availHyperThreads: Int = Runtime.getRuntime.availableProcessors()

  lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Odibel")
    sparkConf.setMaster(s"local[$availHyperThreads]")
  }

}
