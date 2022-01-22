package apache.berry.tutorial.spark.sql

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

object InferSchema extends  App{
  val inferSchemaJsonFile = this.getClass.getResource("/Data/SparkSQL/inferSchema.json").toString

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Infer Schema")
    .getOrCreate()

  val df = spark.read.json(inferSchemaJsonFile)

  df.printSchema()
  df.show()
  /*
  root
   |-- sale_ts: timestamp (nullable = true)
   |-- ticket_total_value: long (nullable = true)
   |-- title: string (nullable = true)

  +-------------------+------------------+----------------+
  |            sale_ts|ticket_total_value|           title|
  +-------------------+------------------+----------------+
  |2019-07-18 15:30:00|                12|        Die Hard|
  |2019-07-18 15:31:00|                12|        Die Hard|
  +-------------------+------------------+----------------+
   */

  // Using Struct Type
  import org.apache.spark.sql.types._

  val structTypeSchema = StructType(
    StructField("title", StringType, true) ::
      StructField("ticket_total_value", IntegerType, true) ::
      StructField("sales_ts", TimestampType, true) :: Nil)

  val df1 = spark
    .read
    .schema(structTypeSchema)
    .json(inferSchemaJsonFile)

  df1.printSchema()
  df1.show()
  /*
    root
   |-- title: string (nullable = true)
   |-- ticket_total_value: integer (nullable = true)
   |-- sales_ts: timestamp (nullable = true)

  +----------------+------------------+--------+
  |           title|ticket_total_value|sales_ts|
  +----------------+------------------+--------+
  |        Die Hard|                12|    null|
  |        Die Hard|                12|    null|
   */
  //Using Encoder
  import org.apache.spark.sql.Encoders

  case class Json(title: String, ticket_total_value: Int, sales_ts: Timestamp )

  val encoderSchema = Encoders.product[Json].schema

  val df2 = spark
    .read
    .schema(encoderSchema)
    .json(inferSchemaJsonFile)

  df2.printSchema()
  df2.show()
  //TODO: Check Later on the timestamp column is not working
  //Using case class explicitly
//  import spark.implicits._
//  val df3 = spark
//    .read
//    .json(inferSchemaJsonFile)
//    .map(row => Json(row.getString(0), row.getString(1).toInt,
//      convertString2TimeStamp(row.getString(2))))
//
//  df3.printSchema()
//  df3.show()


  private def convertString2TimeStamp(str: String): Timestamp = {
    if (str.isEmpty)
      null
    else {
      new Timestamp(str.toLong)
    }
  }
}
