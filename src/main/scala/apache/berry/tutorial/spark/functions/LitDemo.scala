package apache.berry.tutorial.spark.functions

import org.apache.spark.sql.functions._

/**
  * Problem Statement:
  * We have input dataset names.csv having below columns, mentioning username and middle name.
  * name,mname
  * We have to add the middle name in between user first & last name to produce the final full name.
  *
  *
  * Be careful with the use of concat, if any of the values is null you will have an output of null. In your case is
  * better to use concat_ws(" ", col("uname.0"), col("mname"),col("uname.1")) to prevent that behavior and also skip the
  * repetition of the literals with the whitespace.
  *
  *
  * using DataFrame
  *
  *from pyspark.sql.functions import *
  * df.withColumn("arr",split(col('Name')," ")).withColumn("Fname",col('arr')[0]).withColumn("LName",col('arr')[1])
  * .withColumn("Full Name",concat_ws(" ",col('Fname'),col('MiddleName'),col('Lname'))).select(col('Full Name'))
  * .show(truncate=False)
  */

object LitDemo extends App {

  val datapath = this.getClass.getResource("/Data/Sample/names.csv").toString

  val data = spark.read
    .format("csv")
    .option("header", true)
    .option("path", datapath)
    .load()

  data.show(false)

  data
    .withColumn("uname", split(col("name"), " "))
    .select(
      concat(
        col("uname").getItem(0),
        lit(" "),
        col("mname"),
        lit(" "),
        col("uname").getItem(1)
      ).as("full_name")
    )
    .sort("full_name")
    .show(false)

  // Using Concat_ws to handle null values
  data
    .withColumn("uname", split(col("name"), " "))
    .select(
      concat_ws(" ", col("uname").getItem(0), col("mname"), col("uname").getItem(1))
        .as("fullName")
    )
    .sort("fullName")
    .show(false)
}
