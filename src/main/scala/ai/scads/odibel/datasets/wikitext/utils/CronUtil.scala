package ai.scads.odibel.datasets.wikitext.utils

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

// TODO implement in main extraction
// TODO write test
object CronUtil {

  val cronExpression = "0 0 1 1 *"

  // Parse the cron expression
  private val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
  private val parser = new CronParser(cronDefinition)
  private val cron = parser.parse(cronExpression)
  cron.validate()

  def main(args: Array[String]): Unit = {
    // Example input:
    val startEpoch = 1538517832L   // e.g. 2018-10-03T14:23:52Z
    val endEpoch   = 1767351600L   // e.g. 2026-01-01T00:00:00Z

    // The cron pattern: "0 0 1 1 *" means "At 00:00 on January 1, every year"

    val occurrences = findCronOccurrencesBetween(startEpoch, endEpoch, ZoneId.of("UTC"))
    occurrences.foreach(dt => println(dt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
  }

  /**
   * Finds all occurrences of a cron expression between the given start and end epoch seconds.
   * @param cronExpression The cron pattern (e.g. "0 0 1 1 *").
   * @param startEpoch Unix epoch start time (in seconds).
   * @param endEpoch Unix epoch end time (in seconds).
   * @param zoneId Time zone for interpretation.
   * @return A sequence of ZonedDateTime representing all matches.
   */
  def findCronOccurrencesBetween(startEpoch: Long,
                                 endEpoch: Long,
                                 zoneId: ZoneId = ZoneId.systemDefault()): Seq[ZonedDateTime] = {

    // Convert start and end times to ZonedDateTime
    val startTime = Instant.ofEpochSecond(startEpoch).atZone(zoneId)
    val endTime   = Instant.ofEpochSecond(endEpoch).atZone(zoneId)


    val executionTime = ExecutionTime.forCron(cron)

    // Find the first occurrence at or after startTime
    val firstOccurrence = executionTime.nextExecution(startTime)

    // If no initial occurrence is found, return empty
    if (firstOccurrence.isEmpty) {
      return Seq.empty
    }

    // Iterate through occurrences until we pass endTime
    var occurrences = List.empty[ZonedDateTime]
    var currentOccurrenceOpt = firstOccurrence

    while (currentOccurrenceOpt.isPresent) {
      val currentOccurrence = currentOccurrenceOpt.get
      if (currentOccurrence.isAfter(endTime)) {
        // If the occurrence is beyond the end time, stop
        return occurrences
      }
      occurrences = occurrences :+ currentOccurrence
      currentOccurrenceOpt = executionTime.nextExecution(currentOccurrence)
    }

    occurrences
  }
}

