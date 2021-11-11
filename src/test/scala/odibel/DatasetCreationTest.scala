package odibel
import DatasetCreation._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
class DatasetCreationTest extends AnyFunSuite {
  val sparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("ntriple1-reader"))

  test("Creates degree vector") {

    val expectedDegreeVector: Vector[Long] = Vector(2, 2, 0, 1)
    val vectorDegree = degreeVector(readNTriples(sparkContext, inputDataset))
  //  assert(expectedDegreeVector == vectorDegree)
  }
}
