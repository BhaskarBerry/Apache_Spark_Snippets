package apache.berry.tutorial.spark.rdds
/**
 * Syntax : def fold(zeroValue: T)(op: (T, T) => T)
 *
 * zeroValue the initial value for the accumulated result of each partition for the op operator, and
 * also the initial value for the combine results from different partitions for the op operator.
 *
 * Aggregate the elements of each partition, and then the results for all the partitions, using a
 * given associative function and a neutral "zero value".
 *
 * It takes function as an input which has two parameters of the same type and outputs a single value
 * of the input type.
 *
 * Points to Note:
 * 1. fold() is similar to reduce() except it takes a ‘Zero value‘ as an initial value for each partition.
 * 2. fold() is similar to aggregate() with a difference; fold return type should be the same as this RDD element
 * type whereas aggregation can return any type.
 * 3. fold() also same as foldByKey() except foldByKey() operates on Pair RDD
 *
 */

object FoldRDD extends App {
  val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9 ),3)
  println("The Partitions of RDD:"+ rdd.partitions.length)
  val res1 = rdd.glom().collect
  rdd.foreach(println)

  for( i <- 0 to rdd.partitions.length-1) println(res1(i).mkString(" "))

  /*
  rdd data will get distribute across all 3 partitions
  Array[Array[Int]] = Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)).
  // It will get calculate as below.
  // In partition1 => (1+2+3)+2 = 8
  // In partition2 => (4+5+6)+2 = 17
  // In partition3 => (7+8+9)+2 = 26
  // Total across partition = 8+17+26 = 51
  // Combining the result = 51 + 2 = 53
   */
  val sum1 = rdd.fold(2)((acc,value)=> acc + value)
  val sum2 = rdd.fold(0)(_+_)
  println("The Sum1: "+sum1 +"\nThe Sum2: "+sum2)

  //calculate the next highest salary of the emp in company
  val emp = sc.parallelize(List(
    ("Ayub", 50100),
    ("Pavani", 45645),
    ("Berry", 123456),
    ("Bose", 54321),
    ("Ram", 190000)
  ))

  val refEmp = ("RefEmployee", 50000)

  val nextHighestSal = emp.fold(refEmp)((acc, value) => {
   if (value._2 > acc._2) value else acc
  })
  println("Next Highest Salary: "+nextHighestSal)

  // Calculate the largest number
  val numberList = sc.parallelize(List(1,2,3,4,5,6,7,8,88,33,44,550,98))
  val largestNumber = numberList.fold(0)((acc, value) => {
    Math.max(acc, value)
  })
  println("The Largest Number: "+ largestNumber)
}
