package apache.berry.tutorial.spark.rdds

/**
 * def groupBy[K: ClassTag](f: T => K): RDD[(K, Iterable[T])]
 * def groupBy[K: ClassTag](f: T => K, numPartitions: Int): RDD[(K, Iterable[T])]
 * def groupBy[K: ClassTag](f: T => K, p: Partitioner): RDD[(K, Iterable[T])]
 *
 * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
 * mapping to that key. The ordering of elements within each group is not guaranteed, and
 * may even differ each time the resulting RDD is evaluated.
 *
 * groupBy() can be used in both unpaired & paired RDDs. When used with unpaired data, the key
 * for groupBy() is decided by the function literal passed to the method.
 */
object GroupByRDD extends App {

  //1. Group the ages into 3 groups "child", "adult" amd "senior"
  val ageRdd = sc.parallelize(List(1, 12, 35, 36, 45, 15, 56, 85, 98, 45, 55, 25, 16, 34, 53, 21, 14, 6, 8, 9, 75))

  val groupByAge = ageRdd.groupBy(age => {
    if (age >= 18 && age <= 60) "Adult"
    else if (age < 18) "Child"
    else "Senior"
  })
  println("Grouped Age :")
  groupByAge.collect().foreach(println)
  /*
  Grouped Age :
  (Senior,CompactBuffer(85, 98, 75))
  (Adult,CompactBuffer(35, 36, 45, 56, 45, 55, 25, 34, 53, 21))
  (Child,CompactBuffer(1, 12, 15, 16, 14, 6, 8, 9))
   */

  // 2. Group the odd and even numbers
  val numbersRdd = sc.parallelize(List(6, 5, 4, 78, 12, 45, 97, 345, 145, 57, 15, 35, 65, 54, 84, 12, 4, 57, 9))

  val groupedNumbers = numbersRdd.groupBy(number => {
    number % 2 match {
      case 0 => "Even Numbers"
      case _ => "Odd Numbers"
    }
  })
  println("\nGrouped Numbers :")
  groupedNumbers.collect().foreach(println)

  /*
  Grouped Numbers :
  (Even Numbers,CompactBuffer(6, 4, 78, 12, 54, 84, 12, 4))
  (Odd Numbers,CompactBuffer(5, 45, 97, 345, 145, 57, 15, 35, 65, 57, 9))
   */
}
