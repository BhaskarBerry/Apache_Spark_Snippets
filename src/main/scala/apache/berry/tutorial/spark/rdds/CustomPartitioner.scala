package apache.berry.tutorial.spark.rdds

import apache.berry.tutorial.spark.models.{CustomPartitioner, Event}


/**
 * Custom Partitioning: Spark allows users to create custom partitioners by extending the
 *      default Partitioner class. So that we can specify the data to be stored in each partition.
 */

object CustomPartitioner extends App{
  private val eventData = this.getClass.getResource("/Data/event.txt").toString

  import spark.implicits._

  val eventDS =  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(eventData)
    .as[Event]

  val eventRdd  = eventDS
    .map(e => ( e.organizer,e.budget))
    .rdd
    .partitionBy(new CustomPartitioner(3))

  eventRdd
    .reduceByKey(_ + _)
    .saveAsTextFile("C:\\Documents\\CustPatitioner")
}
