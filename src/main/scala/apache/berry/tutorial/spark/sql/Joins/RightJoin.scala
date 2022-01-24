package apache.berry.tutorial.spark.sql.Joins

import apache.berry.tutorial.spark.sql.{sc, spark}

object RightJoin extends App {

  import spark.implicits._

  case class Customer(customerId: Int, custName: String)

  case class Payment(paymentId: Int, customerId: Int, amount: Int)

  case class RightJoinedRows[A, B](left: Option[A], right: B)

  val payment = sc.parallelize(Seq(
    (1, 100, 2509090), (2, 200, 456453), (3, 300, 4321), (4, 400, 5678)
  )).toDF("paymentId", "customerId", "amount").as[Payment]

  payment.show(5)
  /*
    +---------+----------+-------+
  |paymentId|customerId| amount|
  +---------+----------+-------+
  |        1|       100|2509090|
  |        2|       200| 456453|
  |        3|       300|   4321|
  |        4|       400|   5678|
  +---------+----------+-------+
   */

  val customer = sc.parallelize(Seq((100, "Bose"), (200, "Ayub"), (300, "Ram"),
    (400, "Jhon"), (500, "Raghu Ram")))
    .toDF("customerId", "custName").as[Customer]
  customer.show(5)
  /*
      +------+---------+
    |customerId| custName|
    +------+---------+
    |   100|     Bose|
    |   200|     Ayub|
    |   300|      Ram|
    |   400|     Jhon|
    |   500|Raghu Ram|
    +------+---------+
   */

  val rjDF1 = payment
    .join(customer, Seq("customerId"), "right")

  rjDF1.show(false)

  val rjDF2 = payment
    .join(customer, Seq("customerId"), "right_outer")

  rjDF2.show(false)

  /*
  +----------+---------+-------+---------+
  |customerId|paymentId|amount |custName |
  +----------+---------+-------+---------+
  |300       |3        |4321   |Ram      |
  |500       |null     |null   |Raghu Ram|
  |100       |1        |2509090|Bose     |
  |400       |4        |5678   |Jhon     |
  |200       |2        |456453 |Ayub     |
  +----------+---------+-------+---------+
   */
  val rjDF3 = payment.as("p")
    .join(customer.as("c"), $"p.customerId" === $"c.customerId", "right_outer")

  rjDF3.show(false)
  /*
    +---------+----------+-------+----------+---------+
  |paymentId|customerId|amount |customerId|custName |
  +---------+----------+-------+----------+---------+
  |3        |300       |4321   |300       |Ram      |
  |null     |null      |null   |500       |Raghu Ram|
  |1        |100       |2509090|100       |Bose     |
  |4        |400       |5678   |400       |Jhon     |
  |2        |200       |456453 |200       |Ayub     |
  +---------+----------+-------+----------+---------+
   */

  //Join With
  val rjDF4 = payment.as("p")
    .joinWith(customer.as("c"), $"p.customerId" === $"c.customerId", "right_outer")
  rjDF4.show(false)
  /*
  +-----------------+----------------+
  |_1               |_2              |
  +-----------------+----------------+
  |[3, 300, 4321]   |[300, Ram]      |
  |null             |[500, Raghu Ram]|
  |[1, 100, 2509090]|[100, Bose]     |
  |[4, 400, 5678]   |[400, Jhon]     |
  |[2, 200, 456453] |[200, Ayub]     |
  +-----------------+----------------+
   */

  val rightOuterJoinedDF1 = payment.as("p")
    .joinWith(customer.as("c"), $"p.customerId" === $"c.customerId", "right_outer")
    .map { case (l, r) => RightJoinedRows(Option(l), r) }

  val bridged = rightOuterJoinedDF1
    .filter(_.left.isDefined)
    .map { x => (x.left.get, x.right) }
  bridged.show(false)
  /*
    +-----------------+-----------+
  |_1               |_2         |
  +-----------------+-----------+
  |[3, 300, 4321]   |[300, Ram] |
  |[1, 100, 2509090]|[100, Bose]|
  |[4, 400, 5678]   |[400, Jhon]|
  |[2, 200, 456453] |[200, Ayub]|
  +-----------------+-----------+
   */

  val bridgedCustomer = bridged.map { case (_, customer: Customer) => customer }
  bridgedCustomer.show(false)

  /*
  +----------+--------+
  |customerId|custName|
  +----------+--------+
  |300       |Ram     |
  |100       |Bose    |
  |400       |Jhon    |
  |200       |Ayub    |
  +----------+--------+
   */
  val bridgedPayment = bridged.map { case (payment: Payment, _) => payment }
  bridgedPayment.show(false)
  /*
  +---------+----------+-------+
  |paymentId|customerId|amount |
  +---------+----------+-------+
  |3        |300       |4321   |
  |1        |100       |2509090|
  |4        |400       |5678   |
  |2        |200       |456453 |
  +---------+----------+-------+
  */
  val unbridged = rightOuterJoinedDF1
    .filter(_.left.isEmpty)
    .map(_.right)

  unbridged.show(false)
  /*
    +----------+---------+
  |customerId|custName |
  +----------+---------+
  |500       |Raghu Ram|
  +----------+---------+
     */
}
