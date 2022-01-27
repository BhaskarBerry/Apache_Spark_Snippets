package apache.berry.tutorial.spark.sql.datasources

object JsonDS extends App{

  val jsonFile = this.getClass.getResource("/Data/SparkSQL/jsonFile.json").toString
  val peopleFile = this.getClass.getResource("/Data/SparkSQL/people.json").toString
  val multiLineFile = this.getClass.getResource("/Data/SparkSQL/multiLine.json").toString

  val jsonDf= spark.read.json(jsonFile)
  val peopleDf= spark.read.json(peopleFile)
  val multiLineDf= spark.read.json(multiLineFile)

  jsonDf.show(false)
  peopleDf.show(false)
  multiLineDf.show(false)
}
