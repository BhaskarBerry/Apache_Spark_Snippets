package apache.berry.tutorial.spark.sql.Joins

import apache.berry.tutorial.spark.models._
import apache.berry.tutorial.spark.sql.{sc, spark}

/**
 * Left Semi Join and Left anti joins
 *
 * - These are the only joins that only have values from the left table.
 * - A left semi join is the same as filtering the left table for only rows with keys present in the right table.
 * - The left anti join also only returns data from the left table, but instead only returns records that
 * are not present in the right table.
 *
 * -  .joinWith API don't support these joins.
 * Default `inner`. Must be one of: `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
 *                 `right`, `right_outer`.
 */
object LeftSemiAndLeftAntiJoins extends App {

  import spark.implicits._

  case class ID(amount: Int, customerName: String)

  val payment = sc.parallelize(Seq(
    (1, 100, 2509090), (2, 200, 456453), (3, 300, 4321), (4, 400, 5678)
  )).toDF("paymentId", "customerId", "amount").as[Payments]
  payment.show()

  val customer = sc.parallelize(Seq((100, "Bose"), (200, "Ayub"), (103, "Ram")))
    .toDF("customerId", "customerName").as[Customer]
  customer.show()
  /*
+---------+----------+-------+
|paymentId|customerId| amount|
+---------+----------+-------+
|        1|       100|2509090|
|        2|       200| 456453|
|        3|       300|   4321|
|        4|       400|   5678|
+---------+----------+-------+

+----------+------------+
|customerId|customerName|
+----------+------------+
|       100|        Bose|
|       200|        Ayub|
|       103|         Ram|
+----------+------------+
 */
  //LEFT SEMI JOIN

  val leftSemiJoinDF1 = payment.as("p")
    .join(customer.as("c"), $"p.customerId" === $"c.customerId", "left_semi")

  leftSemiJoinDF1.show()
  /*
+---------+----------+-------+
|paymentId|customerId| amount|
+---------+----------+-------+
|        1|       100|2509090|
|        2|       200| 456453|
+---------+----------+-------+
 */
  // LEFT ANTI JOIN
  val leftAntiJoinDF2 = payment.as("p")
    .join(customer.as("c"), $"p.customerId" === $"c.customerId", "left_anti")

  leftAntiJoinDF2.show(false)

  val leftAntiJoinDF1 = payment
    .join(customer, Seq("customerId"), "left_anti")
  leftAntiJoinDF1.show(false)

  /*
  +---------+----------+------+
|paymentId|customerId|amount|
+---------+----------+------+
|3        |300       |4321  |
|4        |400       |5678  |
+---------+----------+------+

+----------+---------+------+
|customerId|paymentId|amount|
+----------+---------+------+
|300       |3        |4321  |
|400       |4        |5678  |
+----------+---------+------+
   */
}
