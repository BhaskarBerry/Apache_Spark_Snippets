package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.SparkSession

package object datasources {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .master("local")
    .getOrCreate()

  val sc  = spark.sparkContext
}
