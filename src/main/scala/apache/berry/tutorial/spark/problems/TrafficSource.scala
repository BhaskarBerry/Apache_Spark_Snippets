package apache.berry.tutorial.spark.problems

import org.apache.spark.sql.SparkSession

object TrafficSource {

  private val trafficSourcePath = this.getClass.getResource("/Data/trafficSource.csv").toString

  case class WebLog(UserId: Int, URL: String, Date: String)

  def main(args: Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    getTrafficByURL(spark, "http://adobe.com")
    getTrafficByUser(spark)

  }

  def getTrafficByURL(spark: SparkSession, URLName: String): Unit = {

    import spark.implicits._

    val trafficData = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "\t")
      .csv(trafficSourcePath)
      .map{r => WebLog(r.getString(0).toInt, r.getString(1), r.getString(2))}

    // On DataSet
    trafficData
        .filter($"URL" === URLName)
        .groupBy("URL")
        .count()
        .show(false)

  }

  def getTrafficByUser(spark: SparkSession): Unit = {

    import spark.implicits._

    val trafficData = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("sep", "\t")
      .csv(trafficSourcePath)
      .map{r => WebLog(r.getString(0).toInt, r.getString(1), r.getString(2))}

    // On DataSet
    val ds = trafficData
        .select($"UserId", $"URL")
      //.filter($"URL" === URLName)
      .groupBy("UserId")

      //.show(false)
    ds.count()

  }

}
