package apache.berry.tutorial.spark.sql.Joins

import apache.berry.tutorial.spark.sql.{sc, spark}

object DSJoin extends App {

  import spark.implicits._

  case class Customer(customerId: Int, customerName: String)

  case class Payment(paymentId: Int, custId: Int, amount: Int)

  case class LeftJoinedRows[A, B](left: A, right: Option[B])

  case class OuterJoinedRows[A, B](left: Option[A], right: Option[B])

  val paymentDS = sc.parallelize(Seq((1, 101, 25000), (2, 102, 43354), (3, 103, 76565), (4, 104, 456785)))
    .toDS()
    .toDF("paymentId", "custId", "amount").as[Payment]

  paymentDS.show()
  /*
    +----------+-------+------+
  |paymentId|custId|amount|
  +----------+-------+------+
  |         1|    101| 25000|
  |         2|    102| 43354|
  |         3|    103| 76565|
  |         4|    104|456785|
  +----------+-------+------+
   */

  val customerDS = sc.parallelize(Seq((101, "Berry"), (102, "Bose"), (103, "Ayub"), (4, "Mala")))
    .toDF("customerId", "customerName")
    .as[Customer]

  customerDS.show()
  /*
    +----------+------------+
  |customerId|customerName|
  +----------+------------+
  |       101|       Berry|
  |       102|        Bose|
  |       103|        Ayub|
  |         4|        Mala|
  +----------+------------+
   */

  //Inner Join --Join

  val innerJoinDF1 = paymentDS
    .join(customerDS, $"custId" === $"customerId", "inner")
  innerJoinDF1.show()
  /*
    +---------+------+------+----------+------------+
  |paymentId|custId|amount|customerId|customerName|
  +---------+------+------+----------+------------+
  |        1|   101| 25000|       101|       Berry|
  |        3|   103| 76565|       103|        Ayub|
  |        2|   102| 43354|       102|        Bose|
  +---------+------+------+----------+------------+
   */

  val innerJoinDF2 = paymentDS //Dataset[(Payment, Customer)]
    .joinWith(customerDS, $"custId" === $"customerId", "inner")

  innerJoinDF2.show()
  /*
    +---------------+------------+
  |             _1|          _2|
  +---------------+------------+
  |[1, 101, 25000]|[101, Berry]|
  |[3, 103, 76565]| [103, Ayub]|
  |[2, 102, 43354]| [102, Bose]|
  +---------------+------------+
   */
  val modified1 = innerJoinDF2 //Dataset[Int]
    .map {
      case (p, c) => p.amount * 2
    }
  modified1.show()
  /*
  +------+
  | value|
  +------+
  | 50000|
  |153130|
  | 86708|
  +------+
   */

  val modified2 = innerJoinDF2 //Dataset[Payment]
    .map {
      case (p, c) => p.copy(amount = p.amount * 2)
    }
  modified2.show()
  /*
  +---------+------+------+
  |paymentId|custId|amount|
  +---------+------+------+
  |        1|   101| 50000|
  |        3|   103|153130|
  |        2|   102| 86708|
  +---------+------+------+
   */

  val modified3 = innerJoinDF2 //Dataset[(Payment, Customer)]
    .map {
      case (p, c) => (p.copy(amount = p.amount * 2), c)
    }
  modified3.show()
  /*
    +----------------+------------+
  |              _1|          _2|
  +----------------+------------+
  | [1, 101, 50000]|[101, Berry]|
  |[3, 103, 153130]| [103, Ayub]|
  | [2, 102, 86708]| [102, Bose]|
  +----------------+------------+
   */

  // Left Outer Join
  val leftOuterJoinDF1 = paymentDS // Dataset[(Payment,Customer)]
    .joinWith(customerDS, $"custId" === $"customerId", "left_outer")
    .map {
      case (left, right) => LeftJoinedRows(left, Option(right)) // Dataset[LeftJoinedRows(Payment,Customer)]
    }
  leftOuterJoinDF1.show()
  /*
  +----------------+------------+
  |            left|       right|
  +----------------+------------+
  | [1, 101, 25000]|[101, Berry]|
  | [3, 103, 76565]| [103, Ayub]|
  | [2, 102, 43354]| [102, Bose]|
  |[4, 104, 456785]|        null|
  +----------------+------------+
   */

  val bridged = leftOuterJoinDF1
    .filter(_.right.isDefined)
    .map(x => (x.left, x.right.get)) // Dataset[(Payment,Customer)]

  bridged.show()
  /*
    +---------------+------------+
  |             _1|          _2|
  +---------------+------------+
  |[1, 101, 25000]|[101, Berry]|
  |[3, 103, 76565]| [103, Ayub]|
  |[2, 102, 43354]| [102, Bose]|
  +---------------+------------+
   */

  val unbridged = leftOuterJoinDF1
    .filter(_.right.isEmpty)
    .map(_.left) // Dataset[Payment]

  unbridged.show()
  /*
    +---------+------+------+
  |paymentId|custId|amount|
  +---------+------+------+
  |        4|   104|456785|
  +---------+------+------+
   */

}
