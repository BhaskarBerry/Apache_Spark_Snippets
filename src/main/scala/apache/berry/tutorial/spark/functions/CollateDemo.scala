package apache.berry.tutorial.spark.functions

import org.apache.spark.sql.functions.{col, collect_list, concat_ws}

/**
 * At times, we may have to collate all the values of a column.
*- collect_list() function can be used for merging rows.
 *
 *Problem Statement: We have input dataset visitplans.csv having below columns mentioning the sweets user wants to eat.
*name,sweet
*We would like to have the result listing all the sweets against an user rather than each sweet being listed separately.
*Also, sweet list should be separated by semicolon.
 */
object CollateDemo extends App{

 val data = this.getClass.getResource("/Data/SparkSQL/visitplans.csv").toString()

 val sweetDF = spark.read.format("csv")
   .option("path", data)
   .option("header", true)
   .load()

 sweetDF.show(false)
 // collate sweets and show the results

 sweetDF.groupBy("name")
   .agg(concat_ws(";", collect_list(col("sweet"))).as("sweets"))
   .sort("name")
   .show(false)
}
