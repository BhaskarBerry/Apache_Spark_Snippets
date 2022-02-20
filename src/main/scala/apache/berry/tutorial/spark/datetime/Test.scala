package apache.berry.tutorial.spark.datetime

import apache.berry.tutorial.spark.datetime.DateTimeFormatters.jst_yyyyMM

object Test extends App {

  println(CurrentTime.utcMidNightTimestamp)

  println(Dates.MaxDate)
  println(Dates.MinDate)

}
