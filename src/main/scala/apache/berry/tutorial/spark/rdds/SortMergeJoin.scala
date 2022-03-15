package apache.berry.tutorial.spark.rdds

object SortMergeJoin extends App {

  import spark.implicits._

  val rdd =
    spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 7, 8, 1, 1, 9, 8, 3, 1, 2, 9, 0, 1, 10), 3)
  val ds1 = spark.sqlContext.createDataset(rdd).select($"value".as("Id_value"))
  val ds2 = spark.sqlContext.createDataset(rdd)

//  ds1.show(false)
//  ds2.show(false)

  ds1.join(ds2, $"Id_value" === $"value").explain()

  println(" ")
}
