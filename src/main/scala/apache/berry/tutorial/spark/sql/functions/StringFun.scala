package apache.berry.tutorial.spark.sql.functions

import apache.berry.tutorial.spark.sql.spark
import org.apache.spark.sql.functions._

object StringFun extends App{

  import spark.implicits._

  val capitalDF = Seq(
    ("India", "NewDelhi", 18758765),
    ("India", "NewDelhi", 5875875),
    ("Germany", "Berlin", 456789),
    ("Japan", "Tokyo", 789456),
    ("Spain", "Madrid", 45618)
  ).toDF("country", "capital", "population")

  val testDf = capitalDF
  capitalDF.show()


  println("Ascii function:" + ascii(testDf.col("country")))
}
