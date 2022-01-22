package apache.berry.tutorial.spark.sql

object InnerJoin extends App {

  import spark.implicits._

  sc.setLogLevel("WARN")

  case class Customer(customer_id: Int, customer_name: String)

  case class Payment(payment_id: Int, customer_id: Int, amount: Int)

  case class ID(amount: Int, customer_name: String)

  case class InnerJoinedRows[A, B](left: A, right: B)

  val payment = sc.parallelize(Seq((1, 101, 2345),
    (2, 102, 3245), (3, 103, 4567), (4, 104, 654))).toDS().toDF("payment_id", "customer_id", "amount").as[Payment]

  payment.show()

  val customer = sc.parallelize(Seq((101, "Bose"), (102, "Ayub"), (103, "Ram")))
    .toDF("customer_id", "customer_name").as[Customer]
  customer.show()
  //  customer.filter($"customer_id" === 101 || $"customer_id" === 102).show(false)
  //  customer.filter($"customer_id" =!= 101).show(false)
  //  customer.filter($"customer_name" =!= "Ayub").show(false)

  //Inner Join

  val result = payment
    .as("p")
    .join(customer as ("c"), $"c.customer_id" === $"p.customer_id")
  result.show(false)
  /*+----------+-----------+------+-----------+-------------+
  |payment_id|customer_id|amount|customer_id|customer_name|
  +----------+-----------+------+-----------+-------------+
  |1         |101        |2345  |101        |Bose         |
  |3         |103        |4567  |103        |Ram          |
  |2         |102        |3245  |102        |Ayub         |
  +----------+-----------+------+-----------+-------------+
   */

  val ijResult1 = payment
    .join(customer, "customer_id")
  ijResult1.show(false)

  val ijResult2 = payment
    .join(customer, Seq("customer_id"), "inner")

  ijResult2.show()
  // ijResult1 == ijResult2 both gives same result
  /*
  +-----------+----------+------+-------------+
  |customer_id|payment_id|amount|customer_name|
  +-----------+----------+------+-------------+
  |101        |1         |2345  |Bose         |
  |103        |3         |4567  |Ram          |
  |102        |2         |3245  |Ayub         |
  +-----------+----------+------+-------------+
   */

  val ijResult3 = payment
    .as("p")
    .join(customer as ("c"), $"c.customer_id" === $"p.customer_id", "inner")
  ijResult3.show(false)
  /*
    +----------+-----------+------+-----------+-------------+
  |payment_id|customer_id|amount|customer_id|customer_name|
  +----------+-----------+------+-----------+-------------+
  |1         |101        |2345  |101        |Bose         |
  |3         |103        |4567  |103        |Ram          |
  |2         |102        |3245  |102        |Ayub         |
  +----------+-----------+------+-----------+-------------+
   */

  val ijResult4 = payment
    .as("p")
    .joinWith(customer as ("c"), $"c.customer_id" === $"p.customer_id", "inner")
  ijResult4.show(false)
  /*
  +--------------+-----------+
  |_1            |_2         |
  +--------------+-----------+
  |[1, 101, 2345]|[101, Bose]|
  |[3, 103, 4567]|[103, Ram] |
  |[2, 102, 3245]|[102, Ayub]|
  +--------------+-----------+
   */
  ijResult4.map {
    case (left, right) => right.customer_name + left.amount
  }.show(false)

  ijResult4.map {
    case (left, right) => ID(left.amount, right.customer_name)
  }.show(false)

  val ijResult5 = payment.as("p")
    .joinWith(customer.as("c"), $"p.customer_id" === $"c.customer_id", "inner")
    .map {
      case (left, right) => InnerJoinedRows(left, right)
    }

  val bridged = ijResult5
    .map(x => (x.left, x.right))

  bridged.show(false)

  // Write to text file

  //  ijResult5.write
  //    .option("header", true) //header.toString
  //    .option("charset", "windows-31j") // Ignored by spark 2.3.1
  //    .option("sep", ",")
  //    .option("escape", "\\")
  //    .option("ignoreLeadingWhiteSpace", "false")
  //    .option("ignoreTrailingWhiteSpace", "false")
  //    //.mode(saveMode)
  //    .csv("test_file")


}
