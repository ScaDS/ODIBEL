package ai.scads.odibel.dbpedia.temporal

import ai.scads.odibel.datasets.wikitext.data.TemporalExtractionResult
import ai.scads.odibel.datasets.wikitext.eval.EvalFunctions
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class TKGTestSuite extends AnyFunSuite {

  // SparkSession Setup
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("TKGEvalTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Testdaten laden
  lazy val testData: Dataset[TemporalExtractionResult] = spark.read.json("src/test/resources/tkg/")
    .withColumn("tFrom", $"tFrom".cast("long"))
    .withColumn("tUntil", $"tUntil".cast("long"))
    .as[TemporalExtractionResult]

  test("SparkSession is initialized") {
    assert(spark != null, "SparkSession should be initialized")
  }

  test("Test data is loaded correctly") {
    assert(testData.count() > 0, "Test data should not be empty")
  }

  test("countAllUniqueWindows computes unique time windows") {
    val uniqueWindows = EvalFunctions.countAllUniqueWindows(testData)
    assert(uniqueWindows == 8, s"Expected 8 unique time windows, got $uniqueWindows")
  }

  test("countTriplesPerSubject counts all unique triples for each subject") {
    import spark.implicits._
    // Expected Result as DataFrame
    val expectedData = Seq(
      ("http://dbpedia.org/resource/subject1", 4),
      ("http://dbpedia.org/resource/subject2", 3),
      ("http://dbpedia.org/resource/subject3", 5)
    ).toDF("head", "triple_count")

    val actual = EvalFunctions.countTriplesPerSubject(testData).orderBy("head").collect()
    val expected = expectedData.orderBy("head").collect()

    assert(actual.length == expected.length, s"Expected ${expected.length} rows, but got ${actual.length}")

    actual.zip(expected).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

  test("countRevisionsPerPage counts all revisions for each page") {
    import spark.implicits._
    // Expected Result as DataFrame
    val expectedData = Seq(
      ("http://dbpedia.org/resource/subject3", 5)
    ).toDF("head", "triple_count")

    val actual = EvalFunctions.countRevisionsPerPage(testData).orderBy("head").collect()
    val expected = expectedData.orderBy("head").collect()

    assert(actual.length == expected.length, s"Expected ${expected.length} rows, but got ${actual.length}")

    actual.zip(expected).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

  test("countChangesPerPredicate counts all changes for each predicate") {
    import spark.implicits._
    // Expected Result as DataFrame
    val expectedData = Seq(
      ("http://dbpedia.org/resource/subject1", "http://dbpedia.org/ontology/relation1", 3, 3),
      ("http://dbpedia.org/resource/subject1", "http://dbpedia.org/ontology/relation2", 1, 1),
      ("http://dbpedia.org/resource/subject2", "http://dbpedia.org/ontology/relation1", 3, 4),
      ("http://dbpedia.org/resource/subject3", "http://dbpedia.org/ontology/relation1", 2, 2),
      ("http://dbpedia.org/resource/subject3", "http://dbpedia.org/ontology/relation2", 2, 2),
      ("http://dbpedia.org/resource/subject3", "http://dbpedia.org/ontology/wikiPageID", 1, 1),
    ).toDF("head", "relation", "unique_changes", "all_changes")

    val actual = EvalFunctions.countChangesPerPredicate(testData).orderBy("head", "rel").collect()
    val expected = expectedData.orderBy("head").collect()

    assert(actual.length == expected.length, s"Expected ${expected.length} rows, but got ${actual.length}")

    actual.zip(expected).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

  test("createSnapshot creates a Snapshot for a specific timestamp") {
    import spark.implicits._
    // Expected Result as DataFrame
    val timestamp1 = 2000
    val expectedData1 = Seq(
      ("http://dbpedia.org/resource/subject1","http://dbpedia.org/ontology/relation1","http://dbpedia.org/resource/tail3>","1001","1003",1000,3000),
      ("http://dbpedia.org/resource/subject1","http://dbpedia.org/ontology/relation1","http://dbpedia.org/resource/tail2>","1002","1003",2000,3000),
      ("http://dbpedia.org/resource/subject2","http://dbpedia.org/ontology/relation1","http://dbpedia.org/resource/tail1>","2002","2003",2000,3000),
      ("http://dbpedia.org/resource/subject3","http://dbpedia.org/ontology/relation1","http://dbpedia.org/resource/tail2>","3002","3003",2000,4000),
    ).toDF("head", "rel", "tail", "rFrom", "rUntil", "tFrom", "tUntil")
    val timestamp2 = 4500
    val expectedData2 = Seq(
    ("http://dbpedia.org/resource/subject1","http://dbpedia.org/ontology/relation2","http://dbpedia.org/resource/tail4>","1003","1004",3000,99000),
    ("http://dbpedia.org/resource/subject2","http://dbpedia.org/ontology/relation1","http://dbpedia.org/resource/tail1>","2004","2005",4000,99000)
    ).toDF("head", "rel", "tail", "rFrom", "rUntil", "tFrom", "tUntil")

    EvalFunctions.createSnapshot(data = testData, timestamp = timestamp1).orderBy("head", "rel", "rFrom","rUntil").show(false)
    EvalFunctions.createSnapshot(data = testData, timestamp = timestamp2).orderBy("head", "rel", "rFrom","rUntil").show(false)

    val actual1 = EvalFunctions.createSnapshot(data = testData, timestamp = timestamp1).orderBy("head", "rel", "rFrom","rUntil").collect()
    val expected1 = expectedData1.orderBy("head", "rel", "rFrom","rUntil").collect()
    val actual2 = EvalFunctions.createSnapshot(data = testData, timestamp = timestamp2).orderBy("head", "rel", "rFrom","rUntil").collect()
    val expected2 = expectedData2.orderBy("head", "rel", "rFrom","rUntil").collect()

    assert(actual1.length == expected1.length, s"Expected ${expected1.length} rows, but got ${actual1.length}")
    assert(actual2.length == expected2.length, s"Expected ${expected2.length} rows, but got ${actual2.length}")

    actual1.zip(expected1).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
    actual2.zip(expected2).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

  test("countStartRevisionsOverTime") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1>\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val result = EvalFunctions.countStartRevisionsOverTime(data).collect()
    val expected = Array(
      ("2020-12-10 01:00:00", 2),
      ("2023-12-10 01:00:00", 2)
    )
    assert(result.map(row => (row.getString(0), row.getLong(1))) === expected)
  }

  test("countEndRevisionsOverTime") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1>\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val result = EvalFunctions.countEndRevisionsOverTime(data).collect()
    val expected = Array(
      ("2020-12-10 01:00:00", 1),
      ("2023-12-10 01:00:00", 2),
      ("2024-12-10 01:00:00", 1)
    )
    assert(result.map(row => (row.getString(0), row.getLong(1))) === expected)
  }

  test("countStartTriplesOverTime") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1>\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val result = EvalFunctions.countStartTriplesOverTime(data).collect()
    val expected = Array(
      ("2020-12-10 01:00:00", 2),
      ("2023-12-10 01:00:00", 1)
    )
    assert(result.map(row => (row.getString(0), row.getLong(1))) === expected)
  }

  test("countEndTriplesOverTime") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1>\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val result = EvalFunctions.countEndTriplesOverTime(data).collect()
    val expected = Array(
      ("2020-12-10 01:00:00", 1),
      ("2023-12-10 01:00:00", 2),
      ("2024-12-10 01:00:00", 1),
    )
    assert(result.map(row => (row.getString(0), row.getLong(1))) === expected)
  }

  test("countChangesOverTime") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1>\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"\\\"none\\\"@en\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val result = EvalFunctions.countChangesOverTime(data).collect()
    val expected = Array(
      ("2020-12-10 01:00:00", 4),
      ("2023-12-10 01:00:00", 4),
      ("2024-12-10 01:00:00", 2),
    )
    assert(result.map(row => (row.getString(0), row.getLong(1))) === expected)
  }

  test("calculateInDegreeDistributionPerYear") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/subject1\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/subject2\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation2\", \"tail\": \"http://dbpedia.org/resource/subject2\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation3\", \"tail\": \"http://dbpedia.org/resource/subject3\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject3\", \"rel\": \"http://dbpedia.org/ontology/relation3\", \"tail\": \"http://dbpedia.org/resource/subject3\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val actual = EvalFunctions.calculateInDegreeDistributionPerYear(data).collect()

    val expected = Seq(
      (2020, "http://dbpedia.org/resource/subject1", 1),
      (2020, "http://dbpedia.org/resource/subject2", 1),
      (2020, "http://dbpedia.org/resource/tail1", 1),
      (2023, "http://dbpedia.org/resource/subject2", 1),
      (2023, "http://dbpedia.org/resource/subject3", 2),
    ).toDF("year", "entity", "degree").orderBy("year", "entity", "degree")
    val expectedRows = expected.collect()

    assert(actual.length == expectedRows.length, s"Expected ${expectedRows.length} rows, but got ${actual.length}")
    actual.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

  test("calculateOutDegreeDistributionPerYear") {
    val data = spark.read.json(spark.createDataset(Seq(
      "{\"head\": \"http://dbpedia.org/resource/subject1\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/tail1\", \"rFrom\": \"1000\", \"rUntil\": \"1001\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/subject1\", \"rFrom\": \"1000\", \"rUntil\": \"1002\", \"tFrom\": 1607558400000, \"tUntil\": 1607558400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation1\", \"tail\": \"http://dbpedia.org/resource/subject2\", \"rFrom\": \"1002\", \"rUntil\": \"1003\", \"tFrom\": 1607558400000, \"tUntil\": 1702166400000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation2\", \"tail\": \"http://dbpedia.org/resource/subject2\", \"rFrom\": \"1003\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject2\", \"rel\": \"http://dbpedia.org/ontology/relation3\", \"tail\": \"http://dbpedia.org/resource/subject3\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject3\", \"rel\": \"http://dbpedia.org/ontology/relation3\", \"tail\": \"http://dbpedia.org/resource/subject3\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}",
      "{\"head\": \"http://dbpedia.org/resource/subject3\", \"rel\": \"http://dbpedia.org/ontology/relation4\", \"tail\": \"http://dbpedia.org/resource/subject3\", \"rFrom\": \"1002\", \"rUntil\": \"1004\", \"tFrom\": 1702166400000, \"tUntil\": 1733788800000}"
    ))).as[TemporalExtractionResult]
    val actual = EvalFunctions.calculateOutDegreeDistributionPerYear(data).collect()

    val expected = Seq(
      (2020, "http://dbpedia.org/resource/subject1", 1),
      (2020, "http://dbpedia.org/resource/subject2", 2),
      (2023, "http://dbpedia.org/resource/subject2", 2),
      (2023, "http://dbpedia.org/resource/subject3", 2),
    ).toDF("year", "entity", "degree").orderBy("year", "entity", "degree")
    val expectedRows = expected.collect()

    assert(actual.length == expectedRows.length, s"Expected ${expectedRows.length} rows, but got ${actual.length}")
    actual.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow == expectedRow, s"Row mismatch: expected $expectedRow, got $actualRow")
    }
  }

}
