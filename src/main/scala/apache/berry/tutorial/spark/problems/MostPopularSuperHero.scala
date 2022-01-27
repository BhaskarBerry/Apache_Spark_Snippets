package apache.berry.tutorial.spark.problems

import org.apache.log4j._
import org.apache.spark.sql.functions._

object MostPopularSuperHero {

  private val marvelGraphResource = this.getClass.getResource("/Data/Marvel-graph.txt").toString
  private val marvelNamesResource = this.getClass.getResource("/Data/Marvel-names.txt").toString

  case class SuperHeroConnections(superhero_id: Int, connections: Int)
  case class SuperHeroName(superhero_id: Int, name: String)

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    mostPopularSuperHero
  }

  def mostPopularSuperHero(): Unit ={

    import spark.implicits._

    val superHeroConnections = spark.read
      .text(marvelGraphResource)
      .map{ row =>
        val superHeroId = row.getString(0).split("\\s+")
        SuperHeroConnections(superHeroId(0).toInt, superHeroId.length - 1)
      }

    val mostPopularSuperHero = superHeroConnections
      //.filter($"superhero_id" === 859)
      .groupBy("superhero_id")
      .agg(sum("connections") as("connections"))
      .sort($"connections".desc)
      .take(1)

    val names = spark.read
      .option("header", false)
      .option("sep", '\"')
      .text(marvelNamesResource)
      .map(line => parseNames(line.getString(0)))


    names.show(false)

      names.filter(_.superhero_id == mostPopularSuperHero(0).getInt(0))
      .show(false)

  }

  def parseNames(line: String) : SuperHeroName = {
    val fields = line.split('\"')
    SuperHeroName(fields(0).trim().toInt, fields(1))
  }

}
