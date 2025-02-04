package ai.scads.odibel.datasets.wikitext


import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.util
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * Takes a set of graph with version and builds a Temporal Graph using Named Graph (Quad-based) annotation
 */
class TemporalWindowBuilder {

  //  private val metaTriples = ListBuffer[MetaTriple]()
  private val temporalExtractionResults = ListBuffer[TemporalExtractionResult]()
  private val currentTriplesWithStart = new util.HashMap[String, MetaObject]()

  def addGraphVersion(triples: List[String], timestamp: Long)(version: String = timestamp.toString): Unit = {

    val newGraphSet: Set[String] = triples.toSet
    val curGraphSet: Set[String] = currentTriplesWithStart.keySet().asScala.toSet

    val addedTriples = newGraphSet &~ curGraphSet
    val deletedTriples = curGraphSet &~ newGraphSet

    // TODO timestamp and version
    addDiff(addedTriples.toList, deletedTriples.toList, timestamp)(version)
  }

  // This is the important function
  // assumes distinct lists so the diff is already performed
  // (s,p,o,b,e)
  private def addDiff(add: List[String], del: List[String], timestamp: Long)(implicit version: String = timestamp.toString): Unit = {

    del.zipWithIndex.foreach {
      case (t, idx) =>
        // todo timestamp format
        closeWindow(t, timestamp, version)
    }
    add.foreach {
      case (t) =>
        currentTriplesWithStart.put(t, MetaObject(timestamp, version))
    }
    // Use references on current triple
  }

  // TODO remove unused
  //  case class MetaTriple(ntstring: String, startTime: String, endTime: String, version: String, idx: Int)
  case class MetaObject(timestamp: Long, version: String)

  def unwrapNTriple(ntstring: String): (String, String, String) = {
    val Array(s, p, o) = ntstring.split(" ", 3)
    val parsedOElement =
      if (o.startsWith("<")) {
        o.substring(1, o.length - 2)
      } else {
        o.substring(0, o.length - 2)
      }
    (s.substring(1, s.length - 1), p.substring(1, p.length - 1), parsedOElement)
  }

  def closeWindow(ntstring: String, timestamp: Long, version: String): Unit = {
    val metaObject = currentTriplesWithStart.remove(ntstring)
    val rUntil = version
    val tUntil = timestamp

    val (head, rel, tail) = unwrapNTriple(ntstring)

    val ter = TemporalExtractionResult(
      head = head,
      rel = rel,
      tail = tail,
      rFrom = metaObject.version,
      rUntil = rUntil,
      tFrom = metaObject.timestamp,
      tUntil = tUntil
    )
    temporalExtractionResults.append(ter)
    // TODO stream as output?
  }

  def buildEntries(): List[TemporalExtractionResult] = {
    // writeOut cut end time

    val finalTimestamp = Long.MaxValue

    // TODO has to be temporal !!! so dont use add Diff
    addDiff(List(), currentTriplesWithStart.asScala.keys.toList, finalTimestamp)

    // TODO twice?
    temporalExtractionResults.toList
  }
}
