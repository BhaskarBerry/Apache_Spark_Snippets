package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

/**
 * Spark pivot() function is used to pivot/rotate the data from one DataFrame/Dataset column into multiple
 * columns (transform row to column).
 *
 * The PIVOT clause is used for data perspective. We can get the aggregated values based on specific column values,
 * which will be turned to multiple columns used in SELECT clause. The PIVOT clause can be specified after the
 * table name or subquery.
 *
 * PIVOT ( { aggregate_expression [ AS aggregate_expression_alias ] } [ , ... ]
 * FOR column_list IN ( expression_list ) )
 */

object Pivot extends App{
  import spark.implicits._

  val myString = "130_salary"
  val myString1 = myString.replaceFirst("[^.]*_", "")
  println(myString1)

  val employee = Seq(
    (1, "a", "SE", 40000, 130),
    (2, "b", "SSE", 50000, 140),
    (3, "b", "SE", 40000, 130),
    (4, "e", "Lead", 70000, 150),
    (5, "f", "Lead", 75000, 150),
  ) .toDF("id", "name", "designation", "salary", "role")
  employee.printSchema()
  employee.show(false)

  /*
    root
   |-- id: integer (nullable = false)
   |-- name: string (nullable = true)
   |-- designation: string (nullable = true)
   |-- salary: integer (nullable = false)
   |-- role: integer (nullable = false)

  +---+----+-----------+------+----+
  |id |name|designation|salary|role|
  +---+----+-----------+------+----+
  |1  |a   |SE         |40000 |130 |
  |2  |b   |SSE        |50000 |140 |
  |3  |b   |SE         |40000 |130 |
  |4  |e   |Lead       |70000 |150 |
  |5  |f   |Lead       |75000 |150 |
  +---+----+-----------+------+----+
     */
  val pivotedDF = employee
    .groupBy("designation")
    .pivot("role")
    .agg(expr("sum(salary)").as("sumSalary"), expr("count(salary)").as("countSalary"))
  pivotedDF.show(false)
  /*
    +-----------+-------------+---------------+-------------+---------------+-------------+---------------+
  |designation|130_sumSalary|130_countSalary|140_sumSalary|140_countSalary|150_sumSalary|150_countSalary|
  +-----------+-------------+---------------+-------------+---------------+-------------+---------------+
  |SSE        |null         |null           |50000        |1              |null         |null           |
  |Lead       |null         |null           |null         |null           |145000       |2              |
  |SE         |80000        |2              |null         |null           |null         |null           |
  +-----------+-------------+---------------+-------------+---------------+-------------+---------------+
   */

  val renamedColumns = pivotedDF.columns.map(c =>pivotedDF(c).as(c.replaceFirst("[^.]*_", "").toLowerCase()))
  for(number <- 1 until  renamedColumns.length){
    println(number)
    println(renamedColumns(number))
  }

  val pivotedDFColumnRenamed = pivotedDF.select(renamedColumns: _*)

  pivotedDFColumnRenamed.show()
  //val dataFrame = spark

  val df100 = spark.range(100).selectExpr("id % 5 as x", "id % 2 as a", "id as b")
  df100.printSchema()
  df100.show()
  df100
    .groupBy('x)
    .pivot("a")
    .agg(expr("sum(b)").as("blah"), expr("count(b)").as("foo"))
    .show()


  val covidSourceDF = Seq(
    ("India", "Covishield", 1000000, 90000),
    ("India", "Covaxin", 200000, 10000),
    ("India", "Covaxin", 200000, 20000),
    ("India", "SputnicV", 10000, 5000),
    ("USA", "Pfizer", 20000000, 100000),
    ("USA", "Moderna", 1000000, 25000),
    ("UAE", "Pfizer", 1000222, 40000),
    ("Canada", "Pfizer", 1000222, 40000),
    ("Russia", "SputnicV", 95000000, 24000)
  ).toDF("country", "vaccination_name", "first_dose", "second_dose")
  covidSourceDF.show(false)

  val covidDF = covidSourceDF
    .groupBy("vaccination_name")
    .pivot("second_dose" )
    //.agg(sum("first_dose").alias("abc"))
    .agg(expr("sum(first_dose)").as("blah"))
  //.na.fill(0)


  //covidSourceDF.groupby(df_data.id, df_data.type).pivot("date").agg(avg("cost").alias("avg_cost"), first("ship").alias("first_ship")).show()
  covidDF.show(false)
  //  +----------------+-----+------+------+--------+-------+-------+-------+--------+
  //  |vaccination_name|5000 |10000 |20000 |24000   |25000  |40000  |90000  |100000  |
  //  +----------------+-----+------+------+--------+-------+-------+-------+--------+
  //  |Pfizer          |0    |0     |0     |0       |0      |2000444|0      |20000000|
  //  |Moderna         |0    |0     |0     |0       |1000000|0      |0      |0       |
  //  |Covaxin         |0    |200000|200000|0       |0      |0      |0      |0       |
  //  |Covishield      |0    |0     |0     |0       |0      |0      |1000000|0       |
  //  |SputnicV        |10000|0     |0     |95000000|0      |0      |0      |0       |
  //  +----------------+-----+------+------+--------+-------+-------+-------+--------+

  covidSourceDF.createOrReplaceTempView("covid")
  spark.sql(s"select * from covid PIVOT (sum(second_dose) as a for country IN('India' as india, 'USA` as uas'))").show(false)
  //spark.sql(s"select * from person PIVOT (sum(age) as a, avg(class) as c for name IN('John' as john, 'Mike` as mike'))").show(false)
  //  +----------------+-------+--------+-------+--------+
  //  |vaccination_name|India  |Russia  |UAE    |USA     |
  //  +----------------+-------+--------+-------+--------+
  //  |Pfizer          |0      |0       |1000222|20000000|
  //  |Moderna         |0      |0       |0      |1000000 |
  //  |Covaxin         |200000 |0       |0      |0       |
  //  |Covishield      |1000000|0       |0      |0       |
  //  |SputnicV        |10000  |95000000|0      |0       |
  //  +----------------+-------+--------+-------+--------+


  val sourceDf = Seq(
    ("john", "notebook", 2),
    ("gary", "notebook", 3),
    ("john", "small phone", 2),
    ("mary", "small phone", 3),
    ("john", "large phone", 3),
    ("john", "camera", 3)
  ).toDF("salesperson","device", "amount sold")
  sourceDf.show(false)

  val sourceDf1 = sourceDf
    .groupBy("salesperson")
    .pivot("device")
    .agg(sum("amount sold"))

  sourceDf1.show(false)
  //  +-----------+------+-----------+--------+-----------+
  //  |salesperson|camera|large phone|notebook|small phone|
  //  +-----------+------+-----------+--------+-----------+
  //  |gary       |null  |null       |3       |null       |
  //  |mary       |null  |null       |null    |3          |
  //  |john       |3     |3          |2       |2          |
  //  +-----------+------+-----------+--------+-----------+


  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  val df = data.toDF("Product","Amount","Country")
  df.show()

  val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
  pivotDF.show()

  val financialProductsDF = Seq(
    ("insurance", "Janusz", 0),
    ("savings account", "Grażyna", 1),
    ("credit card", "Sebastian", 0),
    ("mortgage", "Janusz", 2),
    ("term deposit", "Janusz", 4),
    ("insurance", "Grażyna", 2),
    ("savings account", "Janusz", 5),
    ("credit card", "Sebastian", 2),
    ("mortgage", "Sebastian", 4),
    ("term deposit", "Janusz", 9),
    ("insurance", "Grażyna", 3),
    ("savings account", "Grażyna", 1),
    ("savings account", "Sebastian", 2),
    ("credit card", "Sebastian", 1)
  ).toDF("category", "name", "count")

  val pivotFinancialProductDF = financialProductsDF
    .groupBy("category")
    .pivot("name")
    .agg(sum("count"))


  pivotFinancialProductDF.show(false)

  pivotFinancialProductDF
    .orderBy("category")
    .na.fill(0)

  val personDF = Seq(
    (100, "John", 30, 1, "Street 1"),
    (200, "Mary", 45, 1, "Street 2"),
    (300, "Mike", 80, 3, "Street 3"),
    (400, "Dan", 50, 4, "Street 4"))
    .toDF("id", "name", "age", "class", "address")

  personDF.createOrReplaceTempView("person")

  spark.sql("select * from person").show(false)

  spark.sql(s"select * from person PIVOT (sum(age) as a, avg(class) as c for name IN('John' as john, 'Mike` as mike'))").show(false)

}
