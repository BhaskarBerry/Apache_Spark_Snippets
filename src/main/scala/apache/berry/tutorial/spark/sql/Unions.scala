package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.Payments


object Unions extends App{
  import spark.implicits._

  val payment1 = sc.parallelize(Seq((1,100,123435 ),(2,200,34564),(3,300,8678), (4,400,76856)))
    .toDS().toDF("paymentId", "customerId", "amount").as[Payments]
  payment1.printSchema()
  payment1.show()

  val payment2 = sc.parallelize(Seq((10,1010,125 ),(20,220,4564),(30,330,58678), (40,440,756),(4,400,76856)))
    .toDS().toDF("paymentId", "customerId", "amount").as[Payments]

  payment2.show()
  payment1.union(payment2).explain()
  payment1.union(payment2).show(false)
  payment1.unionByName(payment2).show(false)

  val payment3 = sc.parallelize(Seq((10,1010,125 ),(20,220,4564),(30,330,58678), (40,440,756),(4,400,76856)))
    .toDF( "customerId","paymentId", "amount")


}
