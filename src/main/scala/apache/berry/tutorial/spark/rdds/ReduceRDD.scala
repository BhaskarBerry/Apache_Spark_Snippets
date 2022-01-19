package apache.berry.tutorial.spark.rdds

/**
 * Scala:
 *
 * The reduce() method is a higher-order function that takes all the elements in a collection (Array, List, etc)
 * and combines them using a binary operation to produce a single value. It is necessary to make sure that
 * operations are commutative and associative.
 *
 * Spark:
 * Reduces the elements of RDD using the specified commutative and associative binary operator.
 *
 * def reduce(f: (T, T) => T): T
 *
 * Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The
 * function should be commutative and associative so that it can be computed correctly in parallel.
 *
 * Points to Note :
 * 1. reduce() is similar to fold() except reduce takes a ‘Zero value‘ as an initial value for each partition.
 * 2. reduce() is similar to aggregate() with a difference; reduce return type should be the same as this RDD
 * element type whereas aggregation can return any type.
 * 3. reduce() also same as reduceByKey() except reduceByKey() operates on Pair RDD
 */
object ReduceRDD extends App {
  val num = sc.parallelize(1 to 5)
  
  val sumOfNum = num.reduce((a,b) => a+b)
  println("Sum of Nmbers 1 to 5 : "+ sumOfNum)

  val maxOfNumber = num.reduce((a,b) => a.max(b))
  println("The max of numbers :"+ maxOfNumber)

  val minOfNum = num.reduce((a,b)=> a min b)
  println("Minimum of Numbers: "+minOfNum)
}
