package apache.berry.tutorial.spark.rdds

object CombineByKey extends App {
  val tuple = sc.parallelize(Seq(("A", 10), ("B", 5), ("C", 8),
    ("A", 11), ("B", 8), ("A", 9)))

  tuple.combineByKey(
    v => (v, 1), //Create combiner
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // Merge Values
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  ) //merge Combiner
    .collect()
    .foreach(println)
}
