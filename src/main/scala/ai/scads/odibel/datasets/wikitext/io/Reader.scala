//package ai.scads.odibel.datasets.wikitext.io
//
//import ai.scads.odibel.utils.HDFSUtil
//
//import java.io.File
//import scala.io.Source
//
//class Reader(uri: String) {
//
//  def getLines: Iterator[String] = {
//
//    val sourceIterator =
//      if (sourcePath.startsWith("hdfs")) {
//        val sourcePathParts = sourcePath.split("/", 4)
//        new HDFSUtil(sourcePathParts.take(3).mkString("/")).getHDFSFileLinesIterator("/" + sourcePathParts.last)
//      } else {
//        val source = Source.fromFile(new File(uri))
//        source.getLines()
//      }
//  }
//
//  def close(): Unit = {
//
//  }
//}
