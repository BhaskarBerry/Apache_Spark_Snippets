package apache.berry.tutorial.spark.rdds

import apache.berry.tutorial.spark.models.Purchases

object ReduceByKeyRDD {

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
      ),3
    )

    val purchasePerMonth = purchaseRdd
      .map(p => (p.custoumerId, (1,p.productPrice))) //(100,(1,30.0)) , (200,(1,20.2))
      .reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)) //(100,(2,75.2)), (400,(2,6612.4))
      .collect()

    purchasePerMonth.foreach(println)
  }

}
