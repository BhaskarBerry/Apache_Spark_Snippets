package apache.berry.tutorial.spark.rdds

import apache.berry.tutorial.spark.models.Purchases
/*
This is similar to reduce by key
 */
object GroupByKeyRDD {
  def main(args: Array[String]): Unit = {
    val purchaseRdd = sc.parallelize(
      List(
        Purchases(100, "Berry", 30.0),
        Purchases(200, "Bose", 20.2),
        Purchases(100, "Srikanth", 45.2),
        Purchases(400, "Manu", 4589.2),
        Purchases(500, "Rahul", 5624.2),
        Purchases(400, "Ayub", 2023.2),
        Purchases(200, "Bob", 987.2)
      )
    )

    val purchasePerMonth = purchaseRdd
      .map(p => (p.custoumerId, p.productPrice)) //(100, 30.0)(200,20.2)
      .groupByKey() //(100,(30.0,45.2)),(200,(20.2,987.2))
      .map(p => (p._1, (p._2.size, p._2.sum))) //(100,(2,75.2)),(200,(2,1007.40000))
      .collect()

    purchasePerMonth.foreach(println)

  }

}
