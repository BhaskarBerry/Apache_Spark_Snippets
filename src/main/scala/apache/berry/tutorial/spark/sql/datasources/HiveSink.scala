package apache.berry.tutorial.spark.sql.datasources

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object HiveSink extends App{
  
  val wareHouseLocation  = new File("spark-warehouse").getAbsolutePath
  
  private val peopleData = this.getClass.getResource("/Data/SparkSQL/people2.csv").toString
  
  val spark = SparkSession
    .builder()
    .appName("Spark Hive !!!")
    .config("spark.sql.warehouse.dir", wareHouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  println(spark.version)
  println(spark.sessionState.conf.bucketingEnabled)


  spark.sql("create table if not exists employee(name STRING, age INT, job STRING) USING hive")
  spark.sql("select * from employee").show()

  //spark.sql("create table if not exists customer(customer_id INT, customer_name STRING) CLUSTERED BY(customer_id) INTO 4 BUCKETS using hive")

  val customer = spark.sql("select * from employee")
  customer.printSchema()
  customer.show(false)

  val employeeDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(peopleData)

  employeeDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employee")

  spark.sql("select * from employee").show()

  // Turn on flag for Hive Dynamic Partition
  spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
  spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  employeeDF.write
    .mode(SaveMode.Overwrite)
    .partitionBy("age")
    .format("hive")
    .saveAsTable("hive_partition_table")

  spark.sql("select * from hive_partition_table where age > 30")
    .show(false)
}
