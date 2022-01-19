package apache.berry.tutorial.spark.rdds

/**
 * 1. mapValues : def mapValues[U](f: (V) ⇒ U): RDD[(K, U)]
 * Pass each value in the key-value pair RDD through a map function without changing the keys;
 * this also retains the original RDD's partitioning.mapValues operates on the value only (the
 * second part of the tuple)
 * In other words, given f: B => C and rdd: RDD[(A, B)], these two are identical.
 *
 * val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }
 * val result: RDD[(A, C)] = rdd.mapValues(f)
 *
 * 2. map : def map[U: ClassTag](f: T => U): RDD[U]
 * Return a new RDD by applying a function to all elements of this RDD.map is applicable for
 * both RDD and PairRDD.map operates on the entire record(tuple of key and value).
 * When we use map() with a Pair RDD, we get access to both Key & value.
 * map takes a function that transform each element of a collection.
 *
 */

object MapAndMapValuesRDD extends App {
  val numRDD = sc.parallelize(List((1, 2), (3, 4), (4, 5)))

  // mapValues wold only pass the values to your function
  val mvRdd = numRDD.mapValues(x => x + 1)
  mvRdd.collect.foreach(println)
  /*
  (1,3)
  (3,5)
  (4,6)
   */

  //When we use map() with a Pair RDD, we get access to both Key & value.
  //map would pass the entire record(tuple of key and value)
  val mapRdd = numRDD.map(x => (x._1 + 1, x._2 + 1))
  mapRdd.collect.foreach(println)
  /*
  (2,3)
  (4,5)
  (5,6)
   */

  //Example
  val numOfWorldCup = sc.parallelize(Seq(("India", 2), ("England", 0),("Australia", 5)))
  //map operates on entire record
  val mapRdd2 = numOfWorldCup.map(x => (x,1))
  mapRdd2.collect.foreach(println) // ((India,2),1) ,  ((England,0),1),  ((Australia,5),1)
  
  //mapValues operates on the values only
  val mapValuesRdd2 = numOfWorldCup.mapValues(x => (x,1))
  mapValuesRdd2.collect.foreach(println) //(India,(2,1)),  (England,(0,1)),  (Australia,(5,1))

}
