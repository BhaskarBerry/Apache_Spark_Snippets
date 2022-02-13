package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.functions.col

/**
 * We can select and work with columns in 3 ways.
 *   1. Using $-notation
 *   2. Referring to DF
 *   3. Using SQL query string
 *   4. Using `col` spark sql function
 *
 * SparkSession enables applications to run SQL queries programmatically
 * and returns the result as a DataFrame.Before we should create temporary/global view.
 *   1. createOrReplaceTempView : Creates a local temporary view using the given name
 *     and these are session scoped and will disappear if the session that creates it terminates.
 *
 *   2. createOrReplaceGlobalTempView : temporary view that is shared among all sessions and
 *     keep alive until the Spark application terminates. Global temporary view is tied to a
 *     system preserved database global_temp.
 *
 */
object BasicDFDemo extends App{

  import spark.implicits._

  val capitalDF = Seq(
    ("India", "NewDelhi", 18758765),
    ("India", "NewDelhi", 5875875),
    ("Germany", "Berlin", 456789),
    ("Japan", "Tokyo", 789456),
    ("Spain", "Madrid", 45618)
  ).toDF("country", "capital", "population")

  val testDf = capitalDF
  capitalDF.show()
  capitalDF.printSchema()
  /*
  +-------+--------+----------+
  |country| capital|population|
  +-------+--------+----------+
  |  India|NewDelhi|  18758765|
  |Germany|  Berlin|    456789|
  |  Japan|   Tokyo|    789456|
  |  Spain| Madriad|     45618|
  +-------+--------+----------+

  root
   |-- country: string (nullable = true)
   |-- capital: string (nullable = true)
   |-- population: integer (nullable = false)
  */
  //1. Using $-Notation

  capitalDF.filter($"population" > 789456 ).show()

  // 2. Referring to Dataframe
  capitalDF.filter(capitalDF("population") >= 789456).show()

  // 3. Using SQL Query String
  capitalDF.filter("population > 789456" ).show()

  //4. Using col spark sql function
  capitalDF.filter(col("population") > 789456).show()

  //Temporary View
  capitalDF.createOrReplaceTempView("Capital_tbl")

  spark.sql("select * from Capital_tbl").show()
  spark.sql("select * from Capital_tbl where population > 789454 ").show()

  // Global View
  capitalDF.createGlobalTempView("Capital_GV")
  spark.sql("select * from global_temp.Capital_GV").show()
  spark.sql("select * from global_temp.Capital_GV where population = 789456").show()
  /*
  +-------+--------+----------+
  |country| capital|population|
  +-------+--------+----------+
  |  India|NewDelhi|  18758765|
  |Germany|  Berlin|    456789|
  |  Japan|   Tokyo|    789456|
  |  Spain|  Madrid|     45618|
  +-------+--------+----------+

  +-------+-------+----------+
  |country|capital|population|
  +-------+-------+----------+
  |  Japan|  Tokyo|    789456|
  +-------+-------+----------+
   */

  testDf.createOrReplaceTempView("TestDFResult")

  spark.sql(
    """Select country, capital, max(population)
      |  from TestDFResult
      |  group by country, capital
      |""".stripMargin
  ).show(false)
}
