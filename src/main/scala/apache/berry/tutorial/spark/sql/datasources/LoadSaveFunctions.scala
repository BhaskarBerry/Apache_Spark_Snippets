package apache.berry.tutorial.spark.sql.datasources

import org.apache.spark.sql.SaveMode

/**
 * Default Datasource is parquet, can change using config spark.sql.sources.default
 *
 * Manually Specifying options : You can also manually specify the data source that will be used
 * along with any extra options that you would like to pass to the data source.
 *   Data sources are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet),
 * but for built-in sources you can also use their short names (json, parquet, jdbc, orc, libsvm, csv, text).
 *
 * Saving to a persistent table :
 *   DF can also be saved as persistent table into Hive Metastore using saveAsTable command.
 * Persistent tables will still exist even after your Spark program has restarted, as long as you
 * maintain your connection to the same metastore. A DataFrame for a persistent table can be created
 * by calling the table method on a SparkSession with the name of the table.
 *   For file-based data source, e.g. text, parquet, json, etc. you can specify a custom table path via
 * the path option, e.g. df.write.option("path", "/some/path").saveAsTable("t"). When the table is dropped,
 * the custom table path will not be removed and the table data is still there. If no custom table path
 * is specified, Spark will write data to a default table path under the warehouse directory. When the table
 * is dropped, the default table path will be removed too.
 */

object LoadSaveFunctions extends App{
  private val peopleData = this.getClass.getResource("/Data/SparkSQL/people2.csv").toString
  private val usersData = this.getClass.getResource("/Data/SparkSQL/users.parquet").toString

  //Default Resource is parquet file
  val userDf = spark.read.load(usersData)

  userDf.select("name", "favorite_color").show(false)

  //C:\Training2021\Apache_Spark_Snippets\names.parquet
  userDf.select("name")
    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save("names.parquet")

  //Manually Specifying options
  val peopleDFCsv = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(peopleData)

  peopleDFCsv.show(false)

  // Saving to a persistent table
  peopleDFCsv.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("people")

  val peopleDF = spark.sql("select * from people")
  peopleDF.show(false)


  //Bucketing, Sorting and Partitioning

  //Bucketing and Sorting
  peopleDF.write
    .mode(SaveMode.Overwrite)
    .bucketBy(30, "name")
    .sortBy("age")
    .saveAsTable("bucketed_employee")

  //Partitioning
  peopleDF.write
    .mode(SaveMode.Overwrite)
    .partitionBy("age")
    .format("parquet")
    .save("employee_part.parquet")

  //Partitioning and Bucketing
  peopleDF.write
    .mode(SaveMode.Overwrite)
    .partitionBy("age")
    .bucketBy(30, "name")
    .saveAsTable("part_buck_employee")

  spark.sql("select * from part_buck_employee").show(false)
}
