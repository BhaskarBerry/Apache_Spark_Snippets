package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * The shuffle join is the default one and is chosen when its alternative, broadcast join, can't be used.
 * Concretely, the decision is made by the org.apache.spark.sql.execution.SparkStrategies.JoinSelection resolver.
 * The shuffle join is made under following conditions:
 *
 *  1. the join is not broadcastable
 *  2. and one of 2 conditions is met:
 *
 *  - either
 *    -> sort-merge join is disabled (spark.sql.join.preferSortMergeJoin = false)
 *    -> the join type is one of: inner (inner or cross), left outer, right outer, left semi, left anti
 *    -> a single partition of given logical plan is small enough to build a hash table - small enough means
 *    here that the estimated size of physical plan for one of joined columns is smaller than the result
 *    of spark.sql.autoBroadcastJoinThreshold * spark.sql.shuffle.partitions. In other words, it's most
 *    worthwhile to hash and shuffle data than to broadcast it to all executors.
 *    -> according to the code comment, creating hash tables is costly and it can be only done when one of
 *    tables is at least 3 times smaller than the other. The "smaller" is often an estimation of the size of
 *    given relation, for instance:
 *             1. local relation (DataFrames created in memory) size is estimated as the sum of the sizes of all
 *             attributes multiplied by the number of rows. For instance, a DataFrame with 20 rows composed of an
 *             IntegerType and StringType has the total size of (8 + 20) * 20 (where 8 is the default size of an
 *             integer and 20 of a string)
 *             2. logical relation (e.g. the one representing database table - JDBCRelation) that, in addition to
 *             attributes size, default uses the value defined in spark.sql.defaultSizeInBytes property
 *             (Long.MaxValue). This value is large by default too block default broadcasting of relations that
 *             physically can represent a big table. If you are interested on some real-life example, go the test
 *             "sort merge join" should "be executed instead of shuffle when the data comes from relational database"
 *
 *        - or:
 *             join keys of left table aren't orderable, i.e. they're different than NullType, AtomicType and
 *             orderables StructType, ArrayType and UserDefinedType
 *
 */
object ShuffleHashJoin extends App{

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Shuffle Join")
    .master("local[*]")
    //.config("spark.sql.autoBroadcastJoinThreshold", "1")
    //.config("spark.sql.join.preferSortMergeJoin", "false")
    .getOrCreate()

  import sparkSession.implicits._

  val inMemoryCustomersDataFrame = Seq(
    (1, "Customer_1")
  ).toDF("id", "login")

  val inMemoryOrdersDataFrame = Seq(
    (1, 1, 50.0d, System.currentTimeMillis()), (2, 2, 10d, System.currentTimeMillis()),
    (3, 2, 10d, System.currentTimeMillis()), (4, 2, 10d, System.currentTimeMillis())
  ).toDF("id", "customers_id", "amount", "date")

  inMemoryCustomersDataFrame.show()
  inMemoryOrdersDataFrame.show()
  /*
  +---+----------+
  | id|     login|
  +---+----------+
  |  1|Customer_1|
  +---+----------+

  +---+------------+------+-------------+
  | id|customers_id|amount|         date|
  +---+------------+------+-------------+
  |  1|           1|  50.0|1643216106815|
  |  2|           2|  10.0|1643216106815|
  |  3|           2|  10.0|1643216106815|
  |  4|           2|  10.0|1643216106815|
  +---+------------+------+-------------+
   */
  val ordersByCustomers = inMemoryOrdersDataFrame.as("o")
    .join(inMemoryCustomersDataFrame.as("c"), $"o.customers_id" === $"c.id")

  ordersByCustomers.foreach( customerOrder => {
    println("> "+customerOrder)
  })

  /**
  shuffle join is executed because:
   * the size of plan is greater than the size of broadcast join configuration (96  > 1):
       96 because: IntegerType (4) + IntegerType (4) + DoubleType (8) + LongType (8)) * 3 = 24 * 4 = 96)
   * merge-sort join is disabled
   * the join type is inner (supported by shuffle join)
   * built hash table is smaller than the cost of broadcast (96 < 1 * 200, where 1 is spark.sql.autoBroadcastJoinThreshold
       and 200 is the default number of partitions)
   * one of tables is at least 3 times smaller than the other (72 <= 96, where 72 is the size of customers
       table*3 and 96 is the total place taken by orders table)
   */
  val queryExecution = ordersByCustomers.queryExecution.toString()
  println(s"> ${queryExecution}")

 /** * With below configuration broadcastHashJoin would executed.
    *
  * val sparkSession: SparkSession = SparkSession.builder()
  * .appName("Spark shuffle join")
  * .master("local[*]")
  * //.config("spark.sql.autoBroadcastJoinThreshold", "1")
  * .config("spark.sql.join.preferSortMergeJoin", "false")
  * .getOrCreate()
  *
  */
}
