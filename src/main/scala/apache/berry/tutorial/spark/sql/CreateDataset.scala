package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.Company
import org.apache.spark.sql.Row

/**
 * Dataset provides the benefits of RDDs (strong typing, ability to use powerful lambda functions)
 * with the benefits of Spark SQL’s optimized execution engine.
 *
 *   Dataset = RDD(Strongly typed) + Spark SQL's Optimizations
 *
 * We can create Dataset in many ways
 *1. Using toDS() :
 *     a. from an RDD
 *     b. from common Scala Types
 * 2. Using createDataset() :
 *     Creates a [[org.apache.spark.sql.Dataset]]from a local Seq/List/RDD of data of a given type. This
 *     method requires an encoder (to convert a JVM object of type `T` to and from the internal Spark
 *     SQL representation) that is generally created automatically through implicits from a `SparkSession`,
 *     or can be created explicitly by calling static methods on [[org.apache.spark.sql.Encoders]].
 *
 * 3. From DataFrame
 */

object CreateDataset extends App{
  import spark.implicits._

  case class Employee(name: String, company: String, salary:Int)

  //1. Using to DS() - from common scala types
  val seqIntDS = Seq(1,2,3,4,5).toDS()
  val listIntDS = List(1,2,3,4,5,6,7,8).toDS()
  val mapDS = Map((1, "One"), (2, "Two"), (3, "Three"), (4, "Four")).toList.toDS()
  val arrayDS = Array(1,2,3,4,5,6).toSeq.toDS()
  val seqCaseClassDS = Seq(
    Employee("Bose", "Berry4Tut", 100000),
    Employee("Bhaskar", "Amazon", 300000),
    Employee("Mala", "Car", 500000),
    Employee("Ram", "Cisco", 160000),
    Employee("Ayub", "Dell", 700000),
  ).toDS()

  // from an RDD
  val rddIntDS = sc.parallelize(Seq(1,2,3,4,5)).toDS()
  val KeyValueRDD = sc.parallelize(Seq((1, "spark"), (2, "DB"), (3,"Note"))).toDS()

  //2. Using CreateDataset
  //Using Seq[T]
  val seqDS = spark.createDataset(Seq(1,2,3,4,5,6))
  //Using List[T]
  val listDS = spark.createDataset(List(1,2,3,4,5,6))
  //Using RDD[T]
  val rddDS = spark.createDataset(sc.parallelize(Seq(1,2,3,4)))


  //3. From Dataframe
  //Using .as[T] --> By implicit conversions
  val companySeq = Seq(Company("SparkSQL", 1998, "Larry and Brin", 98880),
    Company("Berry4Tech", 2012, "Bhaskar Berry", 3),
    Company("Microsoft", 1975, "Bill Gates", 303254),
    Company("Amazon", 1990, "Jeff Bezos", 64512)
  )

  val companyDF = sc.parallelize(companySeq).toDF()

  val companyDS = companyDF.as[Company]

  companyDS.show(false)

  /*
    +----------+-------+--------------+----------+
  |name      |founded|founder       |numWorkers|
  +----------+-------+--------------+----------+
  |SparkSQL  |1998   |Larry and Brin|98880     |
  |Berry4Tech|2012   |Bhaskar Berry |3         |
  |Microsoft |1975   |Bill Gates    |303254    |
  |Amazon    |1990   |Jeff Bezos    |64512     |
  +----------+-------+--------------+----------+
   */

  val sparkComponentsRdd = sc.parallelize(Seq((1, "SparkCore"),(2, "SparkSQL"),(3, "SparkStreaming"), (4, "Mlib")))
  val sparkComponentsDF = sparkComponentsRdd.toDF("Id", "SparkComponentName")
  val sparkComponentsDS = sparkComponentsDF.as[(Int, String)]

  val list = sparkComponentsDS.select("SparkComponentName" ).collect().map(_(0)).toList
  sparkComponentsDS.show(false)
  /*
  +---+------------------+
  |Id |SparkComponentName|
  +---+------------------+
  |1  |SparkCore         |
  |2  |SparkSQL          |
  |3  |SparkStreaming    |
  |4  |Mlib              |
  +---+------------------+
   */

  //Using .map()
  val companyDSUsingMap = companyDF.map(row => fromRow(row))
  companyDSUsingMap.show(false)
  /*+----------+-------+--------------+----------+
  |name      |founded|founder       |numWorkers|
  +----------+-------+--------------+----------+
  |SparkSQL  |1998   |Larry and Brin|98880     |
  |Berry4Tech|2012   |Bhaskar Berry |3         |
  |Microsoft |1975   |Bill Gates    |303254    |
  |Amazon    |1990   |Jeff Bezos    |64512     |
  +----------+-------+--------------+----------+
   */

  val map = companyDSUsingMap.map(
    x => Map(x.name -> x.founded)
  )

  map.show(false)
  /*
  +--------------------+
  |value               |
  +--------------------+
  |[SparkSQL -> 1998]  |
  |[Berry4Tech -> 2012]|
  |[Microsoft -> 1975] |
  |[Amazon -> 1990]    |
  +--------------------+
   */
  def fromRow(row: Row): Company = {
    Company(
      name = row.getString(0),
      founded = row.getInt(1),
      founder = row.getString(2),
      numWorkers = row.getInt(3)
    )
  }
}
