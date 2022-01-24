package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.models.Salary
import org.apache.spark.sql.functions._

object GroupBy extends App {

  import spark.implicits._

  val empDs = sc.parallelize(Seq(
    Salary("IT", 100, 33456),
    Salary("IT", 100, 33456),
    Salary("Admin", 200, 23456),
    Salary("Admin", 200, 23456),
    Salary("HR", 300, 83456),
    Salary("HR", 300, 83456)
  )).toDS()

  empDs.show(false)

  empDs
    .groupBy(col("dept"),col("deptId"),col("salary"))
    .agg($"dept".as("temp_dept"), $"deptId".as("temp_deptId"), $"salary".as("temp_salary"))
    .drop("temp_dept","temp_deptId","temp_salary")
    .show(false)
}
