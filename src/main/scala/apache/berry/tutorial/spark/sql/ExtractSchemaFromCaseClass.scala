package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.Company

object ExtractSchemaFromCaseClass extends App {
  //name: String, founded: Int, founder: String, numWorkers
  import spark.implicits._
  val companySeq = Seq(
    Company("Google", 1998, "Page", 14578),
    Company("Amazon", 1990, "Jeff", 4353534),
    Company("Dell", 1990, "Michele Dell", 6589)
  )

  val companyDF = sc.parallelize(companySeq).toDF()
  companyDF.show(false)

  val companyDS = companyDF.as[Company]
  // we assume column names in the database are upper case
  val schemaValue = companyDS.toDF(companyDS.columns.map(_.toUpperCase): _*).schema
  println(schemaValue)
  /*
  StructType(StructField(NAME,StringType,true),
  StructField(FOUNDED,IntegerType,false),
  StructField(FOUNDER,StringType,true),
  StructField(NUMWORKERS,IntegerType,false))

   */
}
