package apache.berry.tutorial.spark.rdds

import org.apache.spark.{HashPartitioner, RangePartitioner}

object HashAndRangePartitioning extends App{
  private val readMeData = this.getClass.getResource("/Data/README.md").toString

  val file = sc.textFile(readMeData)
  
  val words = file
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))

  println(words)

  //HashPartitioner
  val wordWithHP = words
    .partitionBy(new HashPartitioner(6))

  wordWithHP
    .reduceByKey(_ + _)
    .saveAsTextFile("C:\\Documents\\HashPatitioner")

  //RangePartitioner
  val wordWithRP = words
    .partitionBy(new RangePartitioner(6,words))
  wordWithRP
    .reduceByKey(_ + _)
    .saveAsTextFile("C:\\Documents\\RangePatitioner")


}
