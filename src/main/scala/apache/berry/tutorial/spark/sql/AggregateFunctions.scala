package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.functions._

object AggregateFunctions extends App{

  import spark.implicits._

  val footBallDF = Seq(
    ("Messi", 30, 25, 2010),
    ("Ronaldo", 32, 15, 2010),
    ("Neymer", 20, 30, 2010),
    ("Messi", 40, 35, 2011),
    ("Ronaldo", 20, 15, 2011),
  ).toDF("Name", "Goals", "Assists", "Season")

  footBallDF.show()
  /*
  +-------+-----+-------+------+
  |   Name|Goals|Assists|Season|
  +-------+-----+-------+------+
  |  Messi|   30|     25|  2010|
  |Ronaldo|   32|     15|  2010|
  | Neymer|   20|     30|  2010|
  |  Messi|   40|     35|  2011|
  |Ronaldo|   20|     15|  2011|
  +-------+-----+-------+------+
   */
  // Find the total goals scored by each Player and order by desc
  val totalGoals = footBallDF
    .groupBy("Name")
    .agg(sum(col("Goals")).as("Total_Goals"))
    .orderBy(col("Total_Goals").desc)

  totalGoals.show(false)
  /*
  +-------+-----------+
  |Name   |Total_Goals|
  +-------+-----------+
  |Messi  |70         |
  |Ronaldo|52         |
  |Neymer |20         |
  +-------+-----------+
 */
  //Average number of goals and assists for each player in 2010 - '$'
  val avgGoalsAssistsIn2010 = footBallDF
    .filter($"Season" === 2010)
    .groupBy("Name")
    .agg(avg(col("Goals")).as("Avg_Goals"), avg(col("Assists")).as("Avg_Assists"))
    .orderBy($"Avg_Goals".desc)
  avgGoalsAssistsIn2010.show(false)

  //Highest goals scored
  val highestGoals = footBallDF
    .groupBy("Name")
    .agg(max("Goals").as("Max"))
  highestGoals.show(false)
  /*
  +-------+---+
  |Name   |Max|
  +-------+---+
  |Neymer |20 |
  |Ronaldo|32 |
  |Messi  |40 |
  +-------+---+
   */
}
