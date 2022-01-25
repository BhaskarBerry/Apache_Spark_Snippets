package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.Salary
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Window functions operate on a group of rows, referred to as a window, and
 * calculate a return value for each row based on the group of rows.
 *
 * Window functions are useful for processing tasks such as calculating a moving average,
 * computing a cumulative statistic, or accessing the value of rows given the relative
 * position of the current row.
 *
 * Syntax:
 * window_function OVER
 * ( [  { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
 * { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
 * [ window_frame ] )
 */

object WindowsFunction extends App {
  import spark.implicits._

  val empDs = sc.parallelize(Seq(
    Salary("IT", 100, 33456),
    Salary("IT", 100, 43456),
    Salary("Admin", 200, 23456),
    Salary("Admin", 200, 53456),
    Salary("HR", 300, 83456),
    Salary("HR", 300, 43456),
    Salary("HR", 300, 1245),
    Salary("HR", 300, 1245)
  )).toDS()

  empDs.show(false)
  //1. Rank function: rank() : This function will return the rank of each record and skip the subsequent rank
  //following any duplicate rank
  val rank_data = empDs
    .withColumn("rank", rank() over Window.partitionBy("deptId", "dept")
      .orderBy("salary"))
    .withColumn("dense_rank", dense_rank() over Window.partitionBy("deptId", "dept")
      .orderBy("salary"))

  rank_data.show(false)

  /*
    +-----+------+------+----+----------+
    |dept |deptId|salary|rank|dense_rank|
    +-----+------+------+----+----------+
    |IT   |100   |33456 |1   |1         |
    |IT   |100   |43456 |2   |2         |
    |HR   |300   |1245  |1   |1         |
    |HR   |300   |1245  |1   |1         |
    |HR   |300   |43456 |3   |2         |
    |HR   |300   |83456 |4   |3         |
    |Admin|200   |23456 |1   |1         |
    |Admin|200   |53456 |2   |2         |
    +-----+------+------+----+----------+
   */
  /**
   * Window Aggregate Functions
   * Define specification of window. Get aggregated data based on dept
   */

  val byDeptName = Window.partitionBy("dept")

  //Apply Aggregate function on window
  val agg_salary = empDs
    .withColumn("max_salary", max("salary").over(byDeptName))
    .withColumn("min_salary", min("salary").over(byDeptName))
    .select("dept", "max_salary", "min_salary")
    .dropDuplicates()

  agg_salary.show(false)
  /*
  +-----+----------+----------+
  |dept |max_salary|min_salary|
  +-----+----------+----------+
  |HR   |83456     |1245      |
  |Admin|53456     |23456     |
  |IT   |43456     |33456     |
  +-----+----------+----------+

   */

  //Using SQL
  empDs.createOrReplaceTempView("Employee")

  spark
    .sql("select dept, MIN(salary) over (partition by dept) as min_salary," +
      "MAX(salary) over (partition by dept) as max_salary from Employee ")
    .dropDuplicates()
    .show(false)

  //using sql
  spark
    .sql("select dept, deptId, salary, rank() over (PARTITION BY dept ORDER BY salary) as rank from employee")
    .show(false)

  // 2. Dense Rank(dense_rank()): This function will return the rank of each record within a partition but will not
  // skip any rank.
  spark
    .sql("select dept, deptId, salary, dense_rank() over (PARTITION BY dept ORDER BY salary) as dense_rank " +
      "from employee")
    .show(false)

  // 3. Row Number(row_number): This function will assign the row number within the window. If 2 rows will have
  // the same value for ordering column, it is non-deterministic which row number will be assigned to each row
  // with same value.
  val windowSpecification = Window.partitionBy("dept").orderBy("salary")

  val rowNumberDF = empDs
    .withColumn("row_number", row_number().over(windowSpecification))
  rowNumberDF.show(false)

  val rowNumberDF1 = empDs
    .withColumn("row_number", row_number().over(Window.partitionBy("dept")
      .orderBy(col("salary").desc)))
    .show(false)
  /*
  +-----+------+------+----------+
  |dept |deptId|salary|row_number|
  +-----+------+------+----------+
  |HR   |300   |83456 |1         |
  |HR   |300   |43456 |2         |
  |HR   |300   |1245  |3         |
  |HR   |300   |1245  |4         |
  |Admin|200   |53456 |1         |
  |Admin|200   |23456 |2         |
  |IT   |100   |43456 |1         |
  |IT   |100   |33456 |2         |
  +-----+------+------+----------+
   */

  //Using SQL
  spark.sql("select dept , deptId, salary, row_number() over(order by salary) as row_num from Employee " )
    .show(false)
  /*
  +-----+------+------+-------+
  |dept |deptId|salary|row_num|
  +-----+------+------+-------+
  |HR   |300   |1245  |1      |
  |HR   |300   |1245  |2      |
  |Admin|200   |23456 |3      |
  |IT   |100   |33456 |4      |
  |IT   |100   |43456 |5      |
  |HR   |300   |43456 |6      |
  |Admin|200   |53456 |7      |
  |HR   |300   |83456 |8      |
  +-----+------+------+-------+
   */
  spark.sql("select dept , deptId, salary, row_number() over(partition by dept order by salary) as row_num from Employee " )
    .show(false)
/*
+-----+------+------+-------+
|dept |deptId|salary|row_num|
+-----+------+------+-------+
|HR   |300   |1245  |1      |
|HR   |300   |1245  |2      |
|HR   |300   |43456 |3      |
|HR   |300   |83456 |4      |
|Admin|200   |23456 |1      |
|Admin|200   |53456 |2      |
|IT   |100   |33456 |1      |
|IT   |100   |43456 |2      |
+-----+------+------+-------+
 */

  // Without Partition
  val rowNumber2 = empDs
    .withColumn("row_num", row_number() over Window.orderBy("salary"))
  rowNumber2.show(false)

  val maxId = 100

  rowNumber2
    .map{
      x => println(x.getInt(3))
        val newID = maxId + x.getInt(3)
        x.getInt(1) + newID
    }.show(false)

  val secondHighestSal = empDs
    .withColumn("rank", rank() over Window.partitionBy("dept").orderBy("salary"))
    .filter($"rank" === 3)

    secondHighestSal
    .show(false)

  val secondHighestSal1 = empDs
    .withColumn("Denserank", dense_rank() over Window.partitionBy("dept").orderBy("salary"))
    .filter($"Denserank" === 3)

  secondHighestSal1
    .show(false)
  /*
  +----+------+------+----+
|dept|deptId|salary|rank|
+----+------+------+----+
|HR  |300   |43456 |3   |
+----+------+------+----+

+----+------+------+---------+
|dept|deptId|salary|Denserank|
+----+------+------+---------+
|HR  |300   |83456 |3        |
+----+------+------+---------+
   */

  val secondHSWithOutPartitionDF = empDs
    .withColumn("dense_rank", dense_rank() over Window.orderBy("salary"))
  secondHSWithOutPartitionDF.show(false)

  /*
   22/01/25 22:57:52 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition,
   this can cause serious performance degradation.
  +-----+------+------+----------+
  |dept |deptId|salary|dense_rank|
  +-----+------+------+----------+
  |HR   |300   |1245  |1         |
  |HR   |300   |1245  |1         |
  |Admin|200   |23456 |2         |
  |IT   |100   |33456 |3         |
  |IT   |100   |43456 |4         |
  |HR   |300   |43456 |4         |
  |Admin|200   |53456 |5         |
  |HR   |300   |83456 |6         |
  +-----+------+------+----------+
   */
  secondHSWithOutPartitionDF
    .filter(col("dense_rank") === 2)
    .show(false)
}
