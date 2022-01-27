package apache.berry.tutorial.spark.sql.datasources

import apache.berry.tutorial.spark.models.{Customer, Payments}
import org.apache.spark.sql.SaveMode

object HiveWithoutBucketing extends App{
  import spark.implicits._

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  case class ID(amount: Int, customerName: String)


  val payment = sc.parallelize(Seq(
    (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
  )).toDF("paymentId", "customerId", "amount").as[Payments]

  val customer = sc.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam")))
    .toDF("customerId", "customerName").as[Customer]


  payment.write
    //.bucketBy(4, "customerId")
    //.sortBy("customerId")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_payment")

  customer.write
    //.bucketBy(4, "customerId")
    //.sortBy("customerId")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_customer")

  val bucketed_payment = spark.table("bucketed_payment")
  val bucketed_customer = spark.table("bucketed_customer")

  val joined = bucketed_payment.join(bucketed_customer, "customerId")

  joined.show(false)

  Thread.sleep(10000000)
}
