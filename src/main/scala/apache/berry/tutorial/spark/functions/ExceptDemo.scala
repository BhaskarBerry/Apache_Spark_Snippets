package apache.berry.tutorial.spark.functions

/**
 * - While comparing two dataframes, we may want to retain only rows present in 1st dataframe and missing in 2nd dataframe.
 * - "Except" becomes a good choice to handle this requirement.
 *
 * Below is an example to illustrate the usage.
 * Problem Statement: We have input dataset i_movie.csv having below columns listing the movies user like.
 * fname,lname,movie_liked
 * There is another input dataset ni_movie.csv having below columns listing the movies user dislike.
 * fname,lname,movie_disliked
 *
 * It was observed that many users who had reported that they liked certain movie are now disliking it in the latest
 * dislike survey conducted. We want the updated liked movie list by removing the movies that are now being disliked by
 * the user.
 *
 * --minus
 * --Diff function in spark dataframe also will work
 */
object ExceptDemo extends App {

  val idata = this.getClass.getResource("/Data/Movies/iMovies.csv").toString
  val likedDF = spark.read.format("csv")
    .option("path", idata)
    .option("header", value = true)
    .load()

  likedDF.show(false)

  val ddata = this.getClass.getResource("/Data/Movies/dMovies.csv").toString
  val dislikedDF = spark.read.format("csv")
    .option("path", ddata)
    .option("header", value = true)
    .load()

  dislikedDF.show(false)

  // Removes the data which is present in dislikedDF
  likedDF.except(dislikedDF)
    .withColumnRenamed("movie_liked", "LikedMovies")
    .sort("fname", "lname")
    .show(false)


  // Gives same result with minus
  val liketbl: Unit = likedDF.createOrReplaceTempView("Liked")
  val disliketbl: Unit = dislikedDF.createOrReplaceTempView("DisLiked")
  spark.sql("""select *  from Liked
               minus
               select * from DisLiked""".stripMargin)
    .show(false)

  //likedDF.diff(dislikedDF)
}
