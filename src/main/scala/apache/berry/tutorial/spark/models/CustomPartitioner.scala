package apache.berry.tutorial.spark.models

import org.apache.spark.Partitioner

/**
 * numOfPartitions: Int, it takes the number of partitions that needs to be created.
 *
 * getPartition(key: Any): Int, this method will return the particular key to the specified
 * partition ID which ranges from 0 to numPartitions-1 for a given key.
 *
 * Equals(): is the normal java equality method used to compare two objects, this method
 * will test your partitioner object against other objects of itself then it decides whether two
 * of your RDDs are Partitioned in the same way or not.
 *
 * Syntax: rdd.partitionBy(new CustomPartitioner(numberOfPartitioner))
 *
 * Depending on the requirement need to modify custom partitioner and the number of partitions.
 *
 * @param numOfPartitions
 */

class CustomPartitioner(numOfPartitions: Int) extends Partitioner {

  override def numPartitions: Int = numOfPartitions
  override def getPartition(key: Any): Int = key match {
    case "Zomato" => 0
    case "BookMyShow" => 1
    case "Swiggy" => 2
  }

  override def equals(other: Any): Boolean = other match {
    case partitioner: CustomPartitioner =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }

}
