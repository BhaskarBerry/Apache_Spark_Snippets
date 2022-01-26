package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.{Customer, Payments}
import org.apache.spark.sql.functions.broadcast


/**
 * Broadcast join is an important part of Spark SQL’s execution engine. When used, it performs a join on two
 * relations by first broadcasting the smaller one to all Spark executors, then evaluating the join criteria
 * with each executor’s partitions of the other relation.
 *
 * 2 types of Broadcast joins
 *   1. BroadCastHashJoin(BHJ)
 *     - Driver builds in memory hash table to distribute to executors
 *
 *   2. BroadCastNestedLoopJoin(BNLJ)
 *     - Distributes data as array to executors
 *     -  Useful for non-equi joins
 *
 * The broadcast join is controlled through spark.sql.autoBroadcastJoinThreshold configuration entry.
 * This property defines the maximum size of the table being a candidate for broadcast. If the table is much bigger
 * than this value, it won't be broadcasted.
 */
object BroadCastHashJoin extends App{

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

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
  // Get Broadcast Threshold value
  val broadCastThresholdValue = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
  println(broadCastThresholdValue) // 10485760b

  /**
   * You can notice here that, even though Dataframes are small in size sometimes spark doesn’t
   * recognize that the size of the dataframe is < 10 MB. To enforce this we can use the broadcast hint.
   */
  payment.as("p")
    .join(customer.as("c"),
      $"p.customerId" === $"c.customerId", "inner"
    ).explain()
  /*
  == Physical Plan ==
  *(5) SortMergeJoin [customerId#12], [customerId#45], Inner
  :- *(2) Sort [customerId#12 ASC NULLS FIRST], false, 0
  :  +- Exchange hashpartitioning(customerId#12, 200), true, [id=#75]
  :     +- *(1) Project [_1#4 AS paymentId#11, _2#5 AS customerId#12, _3#6 AS amount#13]
  :        +- *(1) SerializeFromObject [knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#4, knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2 AS _2#5, knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3 AS _3#6]
  :           +- Scan[obj#3]
  +- *(4) Sort [customerId#45 ASC NULLS FIRST], false, 0
     +- Exchange hashpartitioning(customerId#45, 200), true, [id=#84]
        +- *(3) Project [_1#40 AS customerId#45, _2#41 AS customerName#46]
           +- *(3) SerializeFromObject [knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1 AS _1#40, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#41]
              +- Scan[obj#39]
   */

  payment.as("p")
    .join(broadcast(customer.as("c")),$"p.customerId" === $"c.customerId", "inner")
    .explain()
  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [customerId#12], [customerId#45], Inner, BuildRight
  :- *(2) Project [_1#4 AS paymentId#11, _2#5 AS customerId#12, _3#6 AS amount#13]
  :  +- *(2) SerializeFromObject [knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._1 AS _1#4, knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._2 AS _2#5, knownnotnull(assertnotnull(input[0, scala.Tuple3, true]))._3 AS _3#6]
  :     +- Scan[obj#3]
  +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint))), [id=#127]
     +- *(1) Project [_1#40 AS customerId#45, _2#41 AS customerName#46]
        +- *(1) SerializeFromObject [knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._1 AS _1#40, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, scala.Tuple2, true]))._2, true, false) AS _2#41]
           +- Scan[obj#39]
   */
}
