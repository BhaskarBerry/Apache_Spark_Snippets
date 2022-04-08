package apache.berry.tutorial.spark.functions

import org.apache.spark.sql.functions.{col, substring_index}

/**
  * Getting text present on left of specified keyword in #spark
  *- substring_index function can be used to get text on left/right of specified delimiter/keyword.
  *
  *Problem statement: We have input text file companies.txt having records specifying details of businesses.
  *We need to get the record details prior to the keyword "Description".
  */
object SubStringDemo extends App {
  //Step-1: Loading the input file
  val companyFilePath = this.getClass.getResource("/Data/FunctionalData/company.txt").toString

  val companiesDF = spark.read
    .format("text")
    .option("path", companyFilePath)
    .load()

  //Step-2: Showing the loaded file
  companiesDF.show(false)

  //Step-3: Getting the data prior to "Description"
  companiesDF
    .withColumn("value", substring_index(col("value"), "|Description", 1))
    .show(false)

  //Note: To get the text present on right of specified keyword, we can use
  companiesDF
    .withColumn("value", substring_index(col("value"), "|Description", -1))
    .show(false)

}
