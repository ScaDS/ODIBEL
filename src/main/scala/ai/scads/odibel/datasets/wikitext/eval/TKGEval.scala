package ai.scads.odibel.datasets.wikitext.eval


import picocli.CommandLine.Command

import java.util.concurrent.Callable

@Command(name = "eval", mixinStandardHelpOptions = true, subcommands = Array(classOf[InputEval], classOf[SnapshotEval], classOf[OutputEval]))
class TKGEval extends Callable[Int] {

  //  @Option(names = Array("--in", "-i"), required = true)
  //  var in: File = _
  //
  //  @Option(names = Array("--out", "-o"), required = true)
  //  var out: File = _
  //
  //  @Option(names = Array("--functions", "-f"), required = false)
  //  var func: Array[String] = _ // TODO filter by functions

  //  def writeOut(name: String, ds: DataFrame): Unit = {
  //    ds.coalesce(1)
  //      .write
  //      .mode("overwrite")
  //      .option("header", "true")
  //      .csv(new File(out, name).getPath)
  //  }

  override def call(): Int = {
    0
  }
}
// Create Spark Session
//    val spark = SparkSession.builder().master("local[*]").getOrCreate()
//    val sql = spark.sqlContext
//
//    import sql.implicits._
//
//    val data = sql.read.json(in.getPath)
//      .withColumn("tFrom", $"tFrom".cast("long"))
//      .withColumn("tUntil", $"tUntil".cast("long"))
//      .as[TemporalExtractionResult]
//
////    val uniqueWindows = EvalFunctions.countAllUniqueWindows(data)
////    import scala.jdk.CollectionConverters._
////    writeOut("uniqueWindows", Seq(uniqueWindows).toDF("uniqueWindows"))
////
////    val triplesPerSubject = EvalFunctions.countTriplesPerSubject(data)
////      .agg(
////        min("triple_count").alias("min"),
////        max("triple_count").alias("max"),
////        avg("triple_count").alias("avg")
////      )
////    writeOut("triplesPerSubject", triplesPerSubject)
////
////    val revisionsPerPage = EvalFunctions.countRevisionsPerPage(data)
////    writeOut("revisionsGroupedByPage",revisionsPerPage)
////
////    val changesPerPredicate = EvalFunctions.countChangesPerPredicate(data)
////    writeOut("changesByPredicate", changesPerPredicate)
////
////    val startRevisionsOverTime = EvalFunctions.countStartRevisionsOverTime(data)
////    writeOut("start_revisions_over_time", startRevisionsOverTime)
////
////    val endRevisionsOverTime = EvalFunctions.countEndRevisionsOverTime(data)
////    writeOut("end_revisions_over_time", endRevisionsOverTime)
////
////    val startTriplesOverTime = EvalFunctions.countStartTriplesOverTime(data)
////    writeOut("start_triples_over_time", startTriplesOverTime)
////
////    val endfTriplesOverTime = EvalFunctions.countEndTriplesOverTime(data)
////    writeOut("end_triples_over_time", endfTriplesOverTime)
////
////    val countChangesOverTime = EvalFunctions.countChangesOverTime(data)
////    writeOut("count_changes_over_time", countChangesOverTime)
////
////
////    val calculateInDegreeFrequency = EvalFunctions.calculateInDegreeFrequency(data)
////    writeOut("calculate_in_degree_frequency", calculateInDegreeFrequency)
////
////    val calculateInDegreeDistributionPerYear = EvalFunctions.calculateInDegreeDistributionPerYear(data)
////    writeOut("calculate_in_degree_per_year", calculateInDegreeDistributionPerYear)
////
////    val calculateOutDegreeFrequency = EvalFunctions.calculateOutDegreeFrequency(data)
////    writeOut("calculate_out_degree_frequency", calculateOutDegreeFrequency)
////
////    val calculateOutDegreeDistributionPerYear = EvalFunctions.calculateOutDegreeDistributionPerYear(data)
////    writeOut("calculate_out_degree_per_year", calculateOutDegreeDistributionPerYear)
////
////
////    val calculateTemporalActivitySpanOverTime = EvalFunctions.calculateTemporalActivitySpanOverTime(data)
////    writeOut("calculate_temporal_activity_span_over_time", calculateTemporalActivitySpanOverTime)
////
////    val calculateTemporalActivitySpan = EvalFunctions.calculateTemporalActivitySpan(data)
////    writeOut("calculate_temporal_activity_span", calculateTemporalActivitySpan)
////
////
//    val countSubjectsOverTime = EvalFunctions.countPartsOfTriplesOverTime(data, count_triple_part = "subject", time_resolution = "monthly")
//    writeOut("countSubjectsOverTime", countSubjectsOverTime)
//    val countPredicatesOverTime = EvalFunctions.countPartsOfTriplesOverTime(data, count_triple_part = "predicate", time_resolution = "monthly")
//    writeOut("countPredicatesOverTime", countPredicatesOverTime)
//    val countObjectsOverTime = EvalFunctions.countPartsOfTriplesOverTime(data, count_triple_part = "object", time_resolution = "monthly")
//    writeOut("countObjectsOverTime", countObjectsOverTime)
////
//
////    val calculateSnapshotStatistics = EvalFunctions.calculateSnapshotStatistics(data)
////    writeOut("calculateSnapshotStatistics", calculateSnapshotStatistics)
//
//    spark.stop()
//    0
//  }
//}
//
//object TKGEval extends App {
//
//  val eval = new TKGEval()
//  eval.in = new File("/home/marvin/workspace/data/dbpedia-tkg/out/tkg_hist13.json")
//  eval.out = new File("/home/marvin/workspace/data/dbpedia-tkg/out/")
//
//  eval.call()
//}
