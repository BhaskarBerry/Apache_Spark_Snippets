package apache.berry.tutorial.spark.problems

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat

object CustomerInsights {

  private val salesResourcePath = this.getClass.getResource("/Data/CustomerInsights/Sales.csv").toString
  private val productResourcePath = this.getClass.getResource("/Data/CustomerInsights/Product.csv").toString
  private val customerResourcePath = this.getClass.getResource("/Data/CustomerInsights/Customer.csv").toString

  case class LeftJoinedRows[A, B](left: A, right: Option[B])

  val DATE_FORMAT = "dd/MM/yyyy HH:mm:ss"

  case class Sales(transaction_id: Int, customer_id: Int, product_id: Int, date: Timestamp, total_amount: Int, total_quantity: Int )
  case class Product(product_id: Int, product_name: String, product_version: String, product_type: String, product_price: Int)
  case class Customer(customer_id: Int, first_name: String, last_name: String , phone_number: Long)

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val customer = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "|")
      .csv(customerResourcePath)
      .map{ r => Customer(r.getString(0).toInt, r.getString(1), r.getString(2), r.getString(3).toLong)}

    val productDS = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "|")
      .csv(productResourcePath)
      .map(row =>
        Product(row.getString(0).toInt, row.getString(1),
          row.getString(2), row.getString(3), row.getString(4).replace("$", "").trim.toInt))

    val salesDS = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "|")
      .csv(salesResourcePath)
      .map(row =>
        Sales(row.getString(0).toInt, row.getString(1).toInt,
          row.getString(2).toInt, convertStringToTimestamp(row.getString(3), DATE_FORMAT),
          row.getString(4).replace("$", "").trim.toInt,
          row.getString(5).toInt))

    productNotSold(spark, productDS, salesDS)
    secondMostPurchase(spark, customer, salesDS)
  }

  def secondMostPurchase(spark: SparkSession, customer: Dataset[Customer], sales: Dataset[Sales]): Unit ={
    import spark.implicits._

    val customerSalesJoin = customer.as("c")
      .join(sales, "customer_id")

      customerSalesJoin.show(false)

    val secondHighestCustomerID = customerSalesJoin
      .select("first_name", "last_name", "customer_id", "total_amount")
      .groupBy("customer_id")
      .agg(sum("total_amount").alias("total_amount"))
      .sort($"total_amount".desc)
      .takeAsList(2).get(1).get(0)

    println("---------- Second most purchase --------------")
    customer
      .filter(customer => customer.customer_id == secondHighestCustomerID)
      .select("first_name", "last_name")
      .show(false)


  }

  def productNotSold(spark: SparkSession, productDS: Dataset[Product], salesDS: Dataset[Sales]): Unit ={

    import spark.implicits._

    val productSalesLeftJoin = productDS.as("p")
      .joinWith(salesDS.as("s"),
        $"p.product_id" === $"s.product_id", "left_outer")
      .map{ case (product, sales) => LeftJoinedRows(product, Option(sales))}

    val bridged = productSalesLeftJoin
      .filter(_.right.isDefined)
      .map(x => (x.left, x.right.get))

    val unbridged = productSalesLeftJoin
      .filter(_.right.isEmpty)
      .map(_.left)

    println("-------- Product not sold -------------")
    unbridged.show(false)

  }

  def convertStringToTimestamp(str: String, dateFormat: String): Timestamp = {
    if (Option(str).getOrElse("").isEmpty) {
      null
    } else {
      val format = new SimpleDateFormat(dateFormat)
      val date = format.parse(str)
      val dateTimeStamp = new Timestamp(date.getTime)
      dateTimeStamp
    }
  }

}
