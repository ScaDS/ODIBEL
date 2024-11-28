package ai.scads.odibel.sample

import com.dimafeng.testcontainers.{GenericContainer, WaitingForService}
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpGet
import org.apache.hadoop.shaded.org.apache.http.impl.client.DefaultHttpClient
//import org.apache.hadoop.shaded.org.apache.http.client
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.wait.strategy.Wait

import java.net.URI
import java.util

class WildScalaTest extends AnyFunSuite {


  val container =
    new GenericContainer(
      "nginx:latest",
      exposedPorts = Seq(80),
      waitStrategy = Some(Wait.forListeningPorts(80))
      //      env = Map("NEO4J_AUTH")
    )

  container.start()
  val nginxWebPort: Int = container.mappedPort(80)
  //  container.exposedPorts(9042)
  //  container.waitingFor(Wait.forListeningPort())
  //  container.start()

  test("spark test") {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val rdd = spark.sparkContext.parallelize(1 until 1000)



    val brdPort = spark.sparkContext.broadcast(nginxWebPort)

    rdd.map(i => {
      val request = new HttpGet(new URI(s"http://localhost:${brdPort.value}"))
      val client = new DefaultHttpClient()
      val response = client.execute(request)

      response.getStatusLine.getStatusCode
    }).collect().foreach(println)
  }

}
