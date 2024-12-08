package ai.scads.odibel.utils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.io.Source
import org.apache.hadoop.io.compress.CompressionCodecFactory

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.URI
import java.time.Duration
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicInteger


class HDFSUtil(namenode: String) {

//  println(s"namenode = $namenode")
  val conf = new Configuration()
  // Set HDFS URI (optional if `core-site.xml` is in the classpath)
  conf.set("fs.defaultFS", namenode) // "hdfs://<namenode_host>:<port>"
  val fs = FileSystem.get(conf)

  def getHDFSFileLinesIterator(filePath: String): Iterator[String] = {
    val path = new Path(filePath)

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(s"File not found: $filePath")
    }

    // Automatically detect the compression codec
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(path)

    // Open file stream, applying codec if compressed
    val inputStream = if (codec != null) {
      codec.createInputStream(fs.open(path))
    } else {
      fs.open(path)
    }

    // Return an iterator for the lines
    Source.fromInputStream(inputStream).getLines()
  }

  def openHDFSFileOutputStream(filePath: String): OutputStream = {
    val path = new Path(filePath)

    // Automatically detect the compression codec based on the file extension
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(path)

    // Create output stream
    if (codec != null) {
      codec.createOutputStream(fs.create(path, true))
    } else {
      fs.create(path, true)
    }
  }

  def listHDFSFiles(directoryPath: String): List[String] = {
    val path = new Path(directoryPath)

    if (!fs.exists(path)) {
      throw new IllegalArgumentException(s"Directory not found: $directoryPath")
    }

    if (!fs.isDirectory(path)) {
      throw new IllegalArgumentException(s"Path is not a directory: $directoryPath")
    }

    // List files recursively
    val files: Array[FileStatus] = fs.listStatus(path)
    files.flatMap { fileStatus =>
      if (fileStatus.isDirectory) {
        listHDFSFiles(fileStatus.getPath.toString) // Recursive call for subdirectories
      } else {
        List(fileStatus.getPath.toString)
      }
    }.toList
  }

  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.hadoop.conf.Configuration

  def ensureParentDirectory(filePath: String): Unit = {
    val path = new Path(new URI(filePath))
    val parentPath = path.getParent

    if (parentPath != null && !fs.exists(parentPath)) {
      // Create the parent directory
      if (fs.mkdirs(parentPath)) {
        println(s"Parent directory created: $parentPath")
      } else {
        throw new RuntimeException(s"Failed to create parent directory: $parentPath")
      }
    }
  }

  def createDir(path: String): Unit = {
    val _path = new Path(new URI(path))
    fs.mkdirs(_path)
  }

  def getFs: FileSystem = {
    fs
  }
}
