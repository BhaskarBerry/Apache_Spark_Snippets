package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * DataFrame is a Dataset organized into named columns. Conceptually, they are equivalent to a table
 * in a relational database or a DataFrame in R or Python but with richer optimizations under the hood.
 *
 * DataFrame has two main advantages over RDD:
 *   1. Optimized execution plans via Catalyst Optimizer.
 *   2. Custom Memory management via Project Tungsten.
 *
 * With a SparkSession, applications can create DataFrames from an
 *   1. existing RDD :
 *       a. Schema reflectively inferred : rdd.toDF()
 *       b. Schema explicitly specified : spark.createDataFrame(rowRDD, schema)
 *   2. from a Hive table,
 *   3. from Spark data sources : Semi-structured/Structured data sources Spark SQL
 *         can directly create dataFrame.
 *         - JSON
 *         - CSV
 *         - JDBC
 *         - Parquet
 */

object CreateDataFrame extends App{
  import spark.implicits._

  private val peopleData1 = this.getClass.getResource("/Data/SparkSQL/people1.csv").toString
  private val peopleData2 = this.getClass.getResource("/Data/SparkSQL/people2.csv").toString

  val data = Seq(("Python", "200000"), ("Scala", "45678"), ("Java", "1457854"))
  val rdd1 = sc.parallelize(data)
  val columns = Seq("Language", "UserCount")

  //1. From existing RDD
  val rdd = Seq(("Scala", 1234), ("Python",2), ("Bi", 4354))

  val rddToDataFrameWithoutColNames = rdd.toDF()
  rddToDataFrameWithoutColNames.printSchema()
  /*
  root
   |-- _1: string (nullable = true)
   |-- _2: integer (nullable = false)
   */

  rddToDataFrameWithoutColNames.show()
  /*
    +------+----+
  |    _1|  _2|
  +------+----+
  | Scala|1234|
  |Python|   2|
  |    Bi|4354|
  +------+----+
   */

  val rddToDataFrameWithColNames = rdd.toDF("Language","Users")
  rddToDataFrameWithColNames.printSchema()
  /*
  root
   |-- Language: string (nullable = true)
   |-- Users: integer (nullable = false)
   */

  rddToDataFrameWithColNames.show()
  /*
  +--------+-----+
  |Language|Users|
  +--------+-----+
  |   Scala| 1234|
  |  Python|    2|
  |      Bi| 4354|
  +--------+-----+
     */
  //Uisng spark ccreateDataFrame() from sparkSession.toDF(columns:_*)

  val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
  dfFromRDD2.show(false)
  /*
    +--------+---------+
  |Language|UserCount|
  +--------+---------+
  |Scala   |1234     |
  |Python  |2        |
  |Bi      |4354     |
  +--------+---------+
   */
  val sample = Seq(
    Row(1, "bat"),
    Row(2, "mouse"),
    Row(3, "Bike")
  )

  val sampleSchema = List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )

  val sampleDF = spark.createDataFrame(
    sc.parallelize(sample),
    StructType(sampleSchema)
  )
  sampleDF.show()

  //3. From Spark DataSource
  val df1 = spark.read
    .option("header", false)
    .csv(peopleData1)

  df1.printSchema()
  df1.show()
  /*
    root
   |-- _c0: string (nullable = true)
   |-- _c1: string (nullable = true)
   |-- _c2: string (nullable = true)

  +-----+---+--------------------+
  |  _c0|_c1|                 _c2|
  +-----+---+--------------------+
  |Jorge| 30|      Java Developer|
  |  Bob| 32|Full Stack Developer|
  +-----+---+--------------------+
   */
  val df2 = spark.read
    .option("header", true)
    .csv(peopleData2)

  df2.printSchema()
  df2.show()
  /*
    root
   |-- name: string (nullable = true)
   |-- age: string (nullable = true)
   |-- job: string (nullable = true)

  +------+---+--------------------+
  |  name|age|                 job|
  +------+---+--------------------+
  | Jorge| 30|      Java Developer|
  |   Bob| 32|Full Stack Developer|
  |Robert| 30|      Java Developer|
  |Edward| 30|   Big Data Engineer|
  | Darek| 32|        Data Analyst|
  +------+---+--------------------+
   */

}
