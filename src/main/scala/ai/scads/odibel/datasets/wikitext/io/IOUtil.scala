package ai.scads.odibel.datasets.wikitext.io

import java.net.URI
import java.nio.file.{Files, Paths, Path => NioPath, StandardOpenOption}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Handles IO from and to different locations/protoocls
 * - file://
 * - hdfs://
 * @param uriString
 */
class IOUtil(uriString: String) {

  // TODO list files
  // TODO read
  // TODO write
  // TODO abstract layers and handle file:/// and hdfs:///

  private val uri = new URI(uriString)
  private val scheme = Option(uri.getScheme)
  private val fs = scheme match {
    case Some("hdfs") =>
      val configuration = new Configuration()
      FileSystem.get(uri, configuration)
    case Some("file") | None =>
      null // Local file system, FileSystem not needed
    case _ =>
      throw new IllegalArgumentException(s"Unsupported scheme: ${uri.getScheme}")
  }

//  def writeData(data: String): Unit = {
//    scheme match {
//      case Some("hdfs") =>
//        val path = new Path(uri)
//        val outputStream = fs.create(path, true) // true to overwrite
//        outputStream.write(data.getBytes("UTF-8"))
//        outputStream.close()
//      case Some("file") | None =>
//        val path = Paths.get(uri)
//        Files.write(path, data.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
//      case _ =>
//        throw new IllegalArgumentException(s"Unsupported scheme: ${uri.getScheme}")
//    }
//  }

  def getParentDirectory: String = {
    scheme match {
      case Some("hdfs") =>
        val parentPath = new Path(uri).getParent
        parentPath.toString
      case Some("file") | None =>
        val parentPath = Paths.get(uri).getParent
        parentPath.toString
      case _ =>
        throw new IllegalArgumentException(s"Unsupported scheme: ${uri.getScheme}")
    }
  }

  def getNewUriFromParent(newName: String): String = {
    val parentDir = getParentDirectory
    s"${scheme.getOrElse("file")}://$parentDir/$newName"
  }

  def getFileName: String = {
    scheme match {
      case Some("hdfs") =>
        new Path(uri).getName
      case Some("file") | None =>
        Paths.get(uri).getFileName.toString
      case _ =>
        throw new IllegalArgumentException(s"Unsupported scheme: ${uri.getScheme}")
    }
  }

  def close(): Unit = {
    if (scheme.contains("hdfs") && fs != null) {
      fs.close()
    }
  }
}

object IOUtil {

}

// Example Usage
//object Main extends App {
//  val hdfsWriter = new FileSystemUtil("hdfs://athena1:9000/user/hofer/tests")
//  println("Parent Directory: " + hdfsWriter.getParentDirectory)
//  println("New URI from Parent: " + hdfsWriter.getNewUriFromParent("subdir/newfile.txt"))
//  println("File Name: " + hdfsWriter.getFileName)
////  hdfsWriter.writeData("Hello, this is a test on HDFS!")
//  hdfsWriter.close()
//
//  val localWriter = new FileSystemUtil("file:///path/to/local/file.txt")
//  println("Parent Directory: " + localWriter.getParentDirectory)
//  println("New URI from Parent: " + localWriter.getNewUriFromParent("subdir/newfile.txt"))
//  println("File Name: " + localWriter.getFileName)
////  localWriter.writeData("Hello, this is a test on local FS!")
//  localWriter.close()
//}
