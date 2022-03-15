package apache.berry.tutorial.spark

import org.apache.spark.sql.SparkSession

package object rdds {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark RDD's")
    .getOrCreate()

  val sc = spark.sparkContext
}
