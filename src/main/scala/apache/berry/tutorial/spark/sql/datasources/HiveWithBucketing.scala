package apache.berry.tutorial.spark.sql.datasources

import apache.berry.tutorial.spark.models.{Customer, Payments}
import org.apache.spark.sql.SaveMode

object HiveWithBucketing extends App{

  import spark.implicits._

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  case class ID(amount:Int, custName: String)


  val paymentDF = sc.parallelize(Seq((1,100,123435 ),(2,200,34564),(3,300,8678), (4,400,76856)))
    .toDS().toDF("paymentId", "customerId", "amount").as[Payments]
  paymentDF.printSchema()
  paymentDF.show()

  val customerDF = sc.parallelize(Seq((100, "Bose"), (200, "Ayub"), (103, "Ram")))
    .toDF("customerId", "customerName").as[Customer]
  customerDF.show()

  paymentDF.write
    .bucketBy(4, "customerId")
    .sortBy("customerId")
    .mode(SaveMode.Overwrite)
    .saveAsTable("Bucketed_Payment")

  customerDF.write
    .bucketBy(4, "customerId")
    .sortBy("customerId")
    .mode(SaveMode.Overwrite)
    .saveAsTable("Bucketed_Customer")

  val bucketedPayment = spark.table("Bucketed_Payment")
  val bucketedCustomer = spark.table("Bucketed_Customer")

  //val joined = bucketedPayment.join(bucketedCustomer, "customer_id")
  val joined = bucketedPayment.as("p")
    .join(bucketedCustomer.as("c"), $"p.customerId" === $"c.customerId", "left_outer")

  val bridged = joined.filter($"c.customerId".isNotNull)
  val unbridged = joined.filter($"c.customerId".isNull)

  joined.show(false)
  bridged.show(false)
  unbridged.show(false)
  bridged.unpersist()

  //  val df1 = joined.select("paymentId", "customerId", "amount")
  //  val df2 = joined.select("customerId", "customerName")

  //  df1.show(false)
  //  df2.show(false)

  Thread.sleep(10000000)


}
