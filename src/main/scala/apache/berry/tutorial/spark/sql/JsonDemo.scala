package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.functions.{col, explode}

/**
Exception in thread "main" org.apache.spark.sql.AnalysisException:
Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the referenced columns only include
the internal corrupt record column (named _corrupt_record by default).
For example: spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count() and
spark.read.schema(schema).json(file).select("_corrupt_record").show().
Instead, you can cache or save the parsed results and then send the same query.
For example, val df = spark.read.schema(schema).json(file).cache() and
then df.filter($"_corrupt_record".isNotNull).count().;

solution:
Option-1: JSON in single line.
Option-2: Add option to read multi line JSON in the code as follows. {"a":{"b":1}}


 - Most of the times, we get nested JSON as input file.
- We need to flatten it so that its easier to do processing & transformations.

Problem Statement: We have input dataset contestants.csv having nested structure as shown in screenshot.
We would like to get flattened dataframe out of it.
 */

object JsonDemo extends App{
  val data = this.getClass.getResource("/Data/Json/contents.json").toString

//  val participantDf = spark.read.format("json")
//    .option("path", data)
//    .load()

  val participantDf = spark.read.option("multiline", "true").json( data)

  participantDf.show(false)
  /*
    +--------------------------------------------------------------------+-------+
  |participants                                                        |trip_id|
  +--------------------------------------------------------------------+-------+
  |[[31, [Bangalore, KA], Bhaskar Berry], [30, [BA, California], Ayub]]|TK001  |
  +--------------------------------------------------------------------+-------+
   */

  // Flatten nested json and show results
  participantDf.select(col("trip_id"),
    explode(col("participants")).as("participant"))
    .withColumn("name",col("participant.name"))
    .withColumn("age",col("participant.age"))
    .withColumn("city",col("participant.location.city"))
    .withColumn("state",col("participant.location.state"))
    .drop("participant")
    .show(false)

  /*
    +-------+-------------+---+---------+----------+
  |trip_id|name         |age|city     |state     |
  +-------+-------------+---+---------+----------+
  |TK001  |Bhaskar Berry|31 |Bangalore|KA        |
  |TK001  |Ayub         |30 |BA       |California|
  +-------+-------------+---+---------+----------+

   */
}
