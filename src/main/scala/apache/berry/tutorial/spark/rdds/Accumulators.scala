package apache.berry.tutorial.spark.rdds

import org.apache.log4j.Logger

/**
 * Spark Accumulators are shared variables which are only “added” through an associative and commutative operation and
 * are used to perform counters (Similar to Map-reduce counters) or sum operations
 *
 * 1.Long Accumulator
 * 2.Double Accumulator
 * 3.Collection Accumulator
 */

object Accumulators extends App {
  val logger = Logger.getLogger(Accumulators.getClass)

  private val logFile = this.getClass.getResource("/Data/apacheLogFile.txt").toString
  println("!!!!!!Long Accumulator!!!!!!")
  val blankLines = sc.longAccumulator("Blank Lines")
  val mozillaUsers = sc.longAccumulator("Mozilla Users")
  val chromeUsers = sc.longAccumulator("Chrome Users")
  val errorCode500 = sc.longAccumulator("500 Error")

  val logFileRdd = sc.textFile(logFile)

  logFileRdd.foreach { line =>
    {
      if (line.isEmpty) blankLines.add(1L)
      else {
        val fields = line.split(" ")
        val website = fields(9)
        val code = fields(6)

        if (website.equals("Mozilla")) mozillaUsers.add(1L)
        if (website.equals("Chrome")) chromeUsers.add(1L)
        if (code.equals("500")) errorCode500.add(1L)
      }
    }
  }

  println(s"\tBlank Lines = ${blankLines.value}")
  println(s"\tNumber of Mozilla users = ${mozillaUsers.value}")
  println(s"\tNumber of Chrome users = ${chromeUsers.value}")
  println(s"\tNumber of Error Response = ${errorCode500.value}")

  println("\n!!!!!!Double Accumulator!!!!!!")
  val rdd = sc.parallelize(Array(1,2,3,4))
  val doubleAcc = sc.doubleAccumulator("DoubleAccumulator!!")
  rdd.foreach(x => doubleAcc.add(x))
  println(s"\tDouble Accumulator = ${doubleAcc.value}")

  println("\n!!!!!!Collection Accumulator!!!!!!")
  val collAcc = sc.collectionAccumulator[Int]("DoubleAccumulator!!")
  rdd.foreach(x => collAcc.add(x))
  println(s"\tCollection Accumulator = ${collAcc.value}")

}
