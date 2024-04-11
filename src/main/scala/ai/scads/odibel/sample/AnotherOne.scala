package ai.scads.odibel.sample

object AnotherOne {
  
    def main(args: Array[String]): Unit = {
        CSVSampler.main(Array("--in", "/workspace/tmp/test.csv", "--samples-of", "primaryName,birthYear,deathYear"))
    }
}
