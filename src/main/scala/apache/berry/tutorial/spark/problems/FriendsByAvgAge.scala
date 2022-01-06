package apache.berry.tutorial.spark.problems

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object FriendsByAvgAge extends App{
  
  val logger = Logger.getLogger(FriendsByAvgAge.getClass)
  
  val spark = SparkSession.builder()
    .appName("Friends By Average Age!!!")
//    .config("spark.eventLog.enabled", "true")
    .master("local")
    .getOrCreate()
  
  val dataPath = this.getClass.getResource("/Data/fakefriends.csv").toString
  
  val lines = spark.sparkContext.textFile(dataPath)

  val rdd = lines.map{
    line => val fields = line.split(",")
      val age =  fields(2).toInt
      val numFriends = fields(3).toInt
      (age,numFriends)
  }
  /**
  Sample Data
  0,Will,33,385
  1,Jean-Luc,26,2
  ----------------------
  mapValues
   0 = {Tuple2@8822} "(33,(385,1))"
   1 = {Tuple2@8823} "(26,(2,1))"
  reduceByKey
   (33,(3904,12))
   */
  val ageByFriends = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val avgByAge = ageByFriends.mapValues(x => x._1 / x._2)

  // collects the results from the RDD (Kicks off the computing the DAG and starts executing the Job)
  val result = avgByAge.collect()

  result.foreach(println)

}
