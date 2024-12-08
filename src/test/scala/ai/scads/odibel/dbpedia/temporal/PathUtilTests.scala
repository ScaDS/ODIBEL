package ai.scads.odibel.dbpedia.temporal

import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.nio.file.{FileSystem, FileSystems, Paths}
import java.util

class PathUtilTests extends AnyFunSuite {

  test("") {

    val uri1 = new URI("hdfs://localhost:8080/parentA/fName1")

//    val path1 = Paths.get(uri1)

    val fs = FileSystems.newFileSystem(uri1,new util.HashMap[String,Any]())

    val p2 = fs.getPath("/user")

    println(p2)

//    println(path1.toUri.getScheme)
//    println(path1.getParent)
  }
}
