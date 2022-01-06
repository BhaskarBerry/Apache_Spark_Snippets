package apache.berry.tutorial.spark.rdds

object OperationsOnPairRDD extends App {

  val rdd = sc.parallelize(
    List(
      "Germany Poland India",
      "India China Bangladesh", "India China UK"
    )
  )

  val wordsRDD = rdd.flatMap(_.split(" "))
  val pairRDD = wordsRDD.map(f => (f, 1))
  pairRDD.foreach(println)

  print("\n!!!!!Distinct- on Pair RDD!!!!")
  pairRDD.distinct.foreach(println)

  print("\n!!!!!SortByKey- on Pair RDD!!!!")
  val sortRDD = pairRDD.sortByKey()
  sortRDD.foreach(println)

  print("\n!!!!!ReduceByKey- on Pair RDD!!!!")
  val wordCount = pairRDD.reduceByKey((a, b) => a + b)
  wordCount.foreach(println)

  def param1 = (accu: Int, v: Int) => accu + v
  def param2 = (accu1: Int, accu2: Int) => accu1 + accu2

  println("\n!!!!!AggregateByKey - on Pair RDD same as Reduce By Key!!!!")
  val wordCount2 = pairRDD.aggregateByKey(0)(param1, param2)
  wordCount2.foreach(println)

  println("\n!!!!Keys!!!!")
  wordCount2.keys.foreach(println)

  println("\n!!!!Values!!!!")
  wordCount2.values.foreach(println)

  println(s"count : ${wordCount2.count()}")
  println("\n!!!!!!Collecting as Map!!!!!!")
  pairRDD.collectAsMap().foreach(println)

}
