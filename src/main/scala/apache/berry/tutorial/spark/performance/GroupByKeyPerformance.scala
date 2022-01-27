package apache.berry.tutorial.spark.performance

import org.apache.spark.sql.SparkSession

object GroupByKeyPerformance {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GroupByKey Example")
      .getOrCreate()

    val start_time = System.nanoTime()

    val file_path = this.getClass.getResource("/Data/README.md").toString
    val fileRdd =
      spark.sparkContext.textFile(file_path)

    val wordCountRdd = fileRdd
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()

    val end_time = System.nanoTime()

    val elapsedTimeInSecond =  (end_time - start_time) / 1000000000
    println("Elapsed time in seconds = " + elapsedTimeInSecond)

  }
}
