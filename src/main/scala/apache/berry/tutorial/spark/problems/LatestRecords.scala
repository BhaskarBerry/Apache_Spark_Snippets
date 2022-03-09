package apache.berry.tutorial.spark.problems

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Finding latest record based on timestamp in spark
  *
  * Problem statement:
  * We have input dataset travels.csv having below columns, describing information of where the traveller was spotted.
  * timestamp,traveller,city,state
  * We need to find the latest location of each traveller based on the timestamp.
  */

object LatestRecords extends App {

  val data = this.getClass.getResource("/Data/travellers.csv").toString

  val travellerDF = spark.read
    .format("csv")
    .option("header", true)
    .option("path", data)
    .load()

  travellerDF.show(false)

  val travellerTimeDF = travellerDF.withColumn("timestamp", to_timestamp(col("timestamp")))

  // creating the required window
  val window = Window
    .partitionBy("traveller")
    .orderBy(desc("timestamp"))

  // Find the last location of the travellers
  travellerTimeDF
    .withColumn("row_num", row_number().over(window))
    .filter(col("row_num") === 1)
    .drop("row_num")
    .sort("traveller")
    .show(false)

}
