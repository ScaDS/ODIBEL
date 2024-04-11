package ai.scads.odibel.sample

import org.apache.spark.sql.SparkSession

object CSVSampler {
  
    def main(args: Array[String]): Unit = {

        val path = 
            if(args.contains("--in"))
                args(args.indexOf("--in")+1)
            else
                throw new IllegalArgumentException("Missing argument --in")

        val samplesOf = 
            if(args.contains("--samples-of"))
                args(args.indexOf("--samples-of")+1).split(",").map(_.trim())
            else
                throw new IllegalArgumentException("Missing argument --samples-of")

        val spark = SparkSession.builder().master("local[*]").appName("ODIBEL").getOrCreate()

        val df = spark.read.csv(path)

        df.printSchema()
    }
}
