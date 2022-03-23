package apache.berry.tutorial.spark

import org.apache.spark.sql.SparkSession

package object jobs {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Excel Data")
    .getOrCreate()

  val sc = spark.sparkContext
}
