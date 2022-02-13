package apache.berry.tutorial.spark.functions

import org.apache.spark.sql.functions.{col, explode, split}

/**
Explode function is useful to convert collection columns to multiple rows so that it can be processed effectively.

Below is an example to illustrate the same.

Problem Statement: We have input dataset choice.csv having below columns
name;fabSweets
Here, column fabSweets is comma-separated sweets that the user likes.
We would like to transform it such that each sweet is on a separate row against the username.
 */
object ExplodeDemo extends App{
  val data =  this.getClass.getResource("/Data/choices.csv").toString

  val sweetDf = spark.read.format("csv")
    .option("path", data)
    .option("header", true)
    .option("sep", ";")
    .load()

  sweetDf.show(true)

  /*
  * +------+--------------------+
  |  name|           fabSweets|
  +------+--------------------+
  | Berry|Laddoo,Jalebi,Mys...|
  |Pavani|Ladoo,Barfi,sugar...|
  +------+--------------------+
  */
  sweetDf.select(col("name"),
    explode(split(col("fabSweets"), ",")).as("fabSweet")).show(false)
  /*
  +------+----------+
  |name  |col       |
  +------+----------+
  |Berry |Laddoo    |
  |Berry |Jalebi    |
  |Berry |Mysore Pak|
  |Pavani|Ladoo     |
  |Pavani|Barfi     |
  |Pavani|sugarCandy|
  +------+----------+
   */

}
