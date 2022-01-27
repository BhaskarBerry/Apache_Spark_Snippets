package apache.berry.tutorial.spark.problems

import apache.berry.tutorial.spark.models.Event
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object AvgBudgetPerEventOrg {

  val logger = Logger.getLogger(FriendsByAvgAge.getClass)

  private val eventResource = this.getClass.getResource("/Data/event.txt").toString

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.eventLog.enabled", true)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val eventDS = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(eventResource)
      .as[Event]

    val eventRDD = eventDS.map(event => (event.organizer, event.budget)).rdd // (org,budget)
    val interMediateRDD = eventRDD.mapValues(x => (x, 1)) // (org,(budget,1))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)) // (org,(budget,#events))

    // (2, 120), (2, 130), (3, 130) => ()

    val avgBudgetRDD = interMediateRDD.mapValues {
      case (budget, numberOfEvents) => budget / numberOfEvents
    }

    println("Average Budget per event organizer---->")
    avgBudgetRDD.collect.foreach(println)

    //Calculate Total Budget for each Organizer.
    val totalBudget = eventRDD.reduceByKey(_ + _)

    println("Total Budget for each organizer---->")
    totalBudget.collect.foreach(println)

  }
}
