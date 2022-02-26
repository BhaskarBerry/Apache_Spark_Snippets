package apache.berry.tutorial.spark.sql.datasources

object MultiLineJson extends App {
  println("without multi line...")

  val data = this.getClass.getResource("/Data/Json/fabBook.json").toString
  spark.read
    .format("json")
    .option("path", data)
    .load()
    .show(false)

  println("with multi line...")
  spark.read
    .format("json")
    .option("path", data)
    .option("multiline", true)
    .load()
    .show(false)

}
