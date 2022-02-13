package apache.berry.tutorial.spark

import org.apache.spark.sql.SparkSession

package object functions {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark SQL")
    .getOrCreate()

  val sc = spark.sparkContext
}
