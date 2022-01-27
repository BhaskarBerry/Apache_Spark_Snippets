package apache.berry.tutorial.spark.sql.datasources

import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel


object DataBaseSink extends App{

  private val peopleData = this.getClass.getResource("/Data/SparkSQL/people2.csv").toString

  val jdbcDF1 = spark
    .read
    .format("jdbc")
    .option("url", "jdbc:h2:~/test")
    .option("dbtable","people" )
    .option("user", "sa")
    .option("password", "sa")
    .load()

  jdbcDF1.show(false)

  jdbcDF1.show(false)

  val properties = new Properties()
  properties.put("user", "sa")
  properties.put("password", "sa")

  val jdbcDF2 = spark.read
    .jdbc("jdbc:h2:~/test", "people", properties)

  jdbcDF2.show(false)

  val peopleDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .load(peopleData)

  peopleDF.show(false)

  peopleDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("url", "jdbc:h2:~/test")
    .option("dbtable", "people")
    .option("user", "sa")
    .option("password", "sa")
    .save()

  spark.read
    .jdbc("jdbc:h2:~/test", "people", properties)
    .show(false)

  import spark.implicits._
  val peopleDF1 = List(("Maven", 38, "Data Analyst")).toDF("name", "age", "job")

  peopleDF1.write
    .mode(SaveMode.Append)
    .jdbc("jdbc:h2:~/test", "people", properties)

  spark.read
    .jdbc("jdbc:h2:~/test", "people", properties)
    .show(false)

  def save[T](toTable: String, dataset: Dataset[T]): Long = {
    val properties = Map(
      "driver" -> "<driver_class>",
      "user" -> "<user_name>",
      "password" -> "<password>"
    )
    val tempDf: DataFrame = dataset.toDF().persist(StorageLevel.MEMORY_AND_DISK)
    val jdbcOptions = new JDBCOptions(url = "<jdbc_url>", table = "<table_name>", properties)

    // we assume column names in the database are upper case
    val schema = tempDf.toDF(tempDf.columns.map(_.toUpperCase): _*).schema
    //JdbcUtils.saveTable(df = tempDf, tableSchema = Some(schema), isCaseSensitive = false, options = jdbcOptions)
    tempDf.count()
  }
}

