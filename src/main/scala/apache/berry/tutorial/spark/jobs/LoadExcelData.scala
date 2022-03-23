package apache.berry.tutorial.spark.jobs

import org.apache.spark.sql.types._

object LoadExcelData extends App {

  val newSchema = StructType(
    List(
      StructField("a", IntegerType, nullable = true),
      StructField("b", StringType, nullable = true),
      StructField("c", StringType, nullable = true)
    )
  )

  val path = this.getClass.getResource("/Data/ExcelData/Test.xlsx").toString

  val data = spark.read
//    .schema(newSchema)
    .format("com.crealytics.spark.excel")
    .option("sheetName", "Sheet1")
    .option("header", "true")
//    .option("treatEmptyValuesAsNulls", "false")
    .option("inferSchema", "false")
//    .option("location", path)
//    .option("addColorColumns", "False")
    .load(path)

  data.show(false)
}
