package apache.berry.tutorial.spark.datetime

import java.sql.Timestamp
import java.time.ZonedDateTime

object Dates {

  val MaxDate: Timestamp = new Timestamp(
    ZonedDateTime.of(3333, 12, 20, 0, 0, 0, 0, TimeZones.UTC).toInstant.toEpochMilli
  )

  val MinDate: Timestamp = new Timestamp(
    ZonedDateTime.of(1800, 12, 20, 0, 0, 0, 0, TimeZones.UTC).toInstant.toEpochMilli
  )
}
