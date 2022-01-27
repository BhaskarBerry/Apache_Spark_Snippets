package apache.berry.tutorial.spark.problems

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SelectScholarship extends App{

  val logger = Logger.getLogger(FriendsByAvgAge.getClass)

  private val demoGraphicResource = this.getClass.getResource("/Data/demographic.txt").toString
  private val financeResource = this.getClass.getResource("/Data/finances.txt").toString

  val spark = SparkSession
    .builder()
    .appName("Select Scholarship")
    //.config("spark.eventLog.enabled", "true")
    .master("local")
    .getOrCreate()

  val dempoGraphicRDD = spark.sparkContext.textFile(demoGraphicResource)
  //val financeRDD = spark.sparkContext.textFile(financeResource).map(Demographic.fromCsvRow())

  //Possibility - 1
  //val eligibleSchlershipEmpCount =

}
