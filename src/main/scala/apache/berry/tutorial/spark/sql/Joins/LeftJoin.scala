package apache.berry.tutorial.spark.sql.Joins

import apache.berry.tutorial.spark.models._
import apache.berry.tutorial.spark.sql.{sc, spark}
import org.apache.spark.sql.functions._

object LeftJoin extends App {
  import spark.implicits._

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

  val leftJoinedDF1 = payment.join(customer, Seq("customerId"), "left")
  leftJoinedDF1.show(false)

  val leftJoinedDF2 = payment.join(customer, Seq("customerId"), "left_outer")
  leftJoinedDF2.show(false)
  /*
    +----------+---------+-------+------------+
  |customerId|paymentId|amount |customerName|
  +----------+---------+-------+------------+
  |300       |3        |4321   |null        |
  |100       |1        |2509090|Bose        |
  |400       |4        |5678   |null        |
  |200       |2        |456453 |Ayub        |
  +----------+---------+-------+------------+
     */

  val bridgedDF1 = leftJoinedDF2
    .filter(col("customerName").isNotNull)
  bridgedDF1.show()
  /*
+----------+---------+-------+------------+
|customerId|paymentId| amount|customerName|
+----------+---------+-------+------------+
|       100|        1|2509090|        Bose|
|       200|        2| 456453|        Ayub|
+----------+---------+-------+------------+
 */
  val unbridgedDF1 = leftJoinedDF2
    .filter(col("customerName").isNull)
  unbridgedDF1.show()
  /*
+----------+---------+------+------------+
|customerId|paymentId|amount|customerName|
+----------+---------+------+------------+
|       300|        3|  4321|        null|
|       400|        4|  5678|        null|
+----------+---------+------+------------+
 */

  val leftJoinedDF3 = payment.as("p").join(customer.as("c"), $"p.customerId" === $"c.customerID", "left_outer")
  leftJoinedDF3.show(false)
  /*
  +---------+----------+-------+----------+------------+
|paymentId|customerId|amount |customerId|customerName|
+---------+----------+-------+----------+------------+
|3        |300       |4321   |null      |null        |
|1        |100       |2509090|100       |Bose        |
|4        |400       |5678   |null      |null        |
|2        |200       |456453 |200       |Ayub        |
+---------+----------+-------+----------+------------+
   */

  val leftJoinedDF4 = payment.as("p").
    joinWith(customer.as("c"), $"p.customerId" === $"c.customerID", "left_outer")
  leftJoinedDF4.show(false)
  /*

+-----------------+-----------+
|_1               |_2         |
+-----------------+-----------+
|[3, 300, 4321]   |null       |
|[1, 100, 2509090]|[100, Bose]|
|[4, 400, 5678]   |null       |
|[2, 200, 456453] |[200, Ayub]|
+-----------------+-----------+
   */

  val leftOuterJoinedDF5 = payment.as("p")
    .joinWith(customer.as("c"), $"p.customerId" === $"c.customerID", "left_outer")
    .map {
      case (left, right) => LeftJoinedRows(left, Option(right))
    }

  leftOuterJoinedDF5.show(false)
  /*
  +-------------------+-----------+
|left               |right      |
+-------------------+-----------+
|[3, 300, 4321.0]   |null       |
|[1, 100, 2509090.0]|[100, Bose]|
|[4, 400, 5678.0]   |null       |
|[2, 200, 456453.0] |[200, Ayub]|
+-------------------+-----------+
   */
  val bridged = leftOuterJoinedDF5
    .filter(_.right.isDefined)
    .map(x => (x.left, x.right.get))

  bridged.show(false)
  /*
+-------------------+-----------+
|_1                 |_2         |
+-------------------+-----------+
|[1, 100, 2509090.0]|[100, Bose]|
|[2, 200, 456453.0] |[200, Ayub]|
+-------------------+-----------+
 */

  val bridgedRight = leftOuterJoinedDF5
    .filter(_.right.isDefined)
    .map(x => x.right.get)

  bridgedRight.show(false)
  /*
+----------+------------+
|customerId|customerName|
+----------+------------+
|100       |Bose        |
|200       |Ayub        |
+----------+------------+
 */
  val bridgedPayment = bridged
    .map {
      case (payment: Payments, _) => payment
    }
  bridgedPayment.show(false)

  val unbriddged = leftOuterJoinedDF5
    .filter(_.right.isEmpty)
    .map(_.left)
  unbriddged.show(false)
  /*
  +---------+----------+------+
|paymentId|customerId|amount|
+---------+----------+------+
|3        |300       |4321.0|
|4        |400       |5678.0|
+---------+----------+------+
   */
}
