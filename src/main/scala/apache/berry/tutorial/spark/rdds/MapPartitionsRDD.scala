package apache.berry.tutorial.spark.rdds

/**
 * mapPartition :
 *   - Return a new RDD by applying a function to each partition of this RDD.
 *
 * mapPartitionWithIndex :
 *   - Return a new RDD by applying a function to each partition of this RDD, while tracking the index
 * of the original partition.
 *
 * Spark mapPartitions - Similar to map() transformation but in this case function runs separately on each
 * partition (block) of RDD unlike map() where it was running on each element of partition. Hence mapPartitions
 * are also useful when you are looking for performance gain (calls your function once/partition not once/element).
 *
 * - Suppose you have elements from 1 to 100 distributed among 10 partitions i.e. 10 elements/partition. map()
 * transformation will call func 100 times to process these 100 elements but in case of mapPartitions(), func will
 * be called once/partition i.e. 10 times.
 *
 * - Secondly, mapPartitions() holds the data in-memory i.e. it will store the result in memory until all the elements
 * of the partition has been processed.
 *
 * - mapPartitions() will return the result only after it finishes processing of whole partition.
 * - mapPartitions() requires an iterator input unlike map() transformation.
 */

object MapPartitionsRDD extends App {

  private val friendsData = this.getClass.getResource("/Data/fakefriends.csv").toString

  val file = sc.textFile(friendsData, 4)

  println(file.partitions.size)

  val partitionsRDD = file.mapPartitions(line => List(line.size).iterator)
  val partitionsRDD1 = file.map(line => line.size)

  partitionsRDD.foreach(println)
  partitionsRDD1.foreach(println)

  println(file.count())

  val mapPartitionWithIndexRDD = file.mapPartitionsWithIndex((index, itr) =>
    {
      println("Partition Number = " + index)
      val myList = itr.toList
      myList.map(x => x + "->" + index).iterator
    })

  mapPartitionWithIndexRDD.collect.foreach(println)

}
