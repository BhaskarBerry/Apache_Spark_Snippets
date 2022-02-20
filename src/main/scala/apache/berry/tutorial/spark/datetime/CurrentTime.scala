package apache.berry.tutorial.spark.datetime

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

object CurrentTime {

  def jstZoned: ZonedDateTime = ZonedDateTime.now(TimeZones.JST)
  def utcZoned: ZonedDateTime = ZonedDateTime.now(TimeZones.UTC)
  def istZoned: ZonedDateTime = ZonedDateTime.now(TimeZones.IST)

  def utcMidNightTimestamp: Timestamp = {
    Timestamp.from(ZonedDateTime.now(TimeZones.UTC).truncatedTo(ChronoUnit.DAYS).toInstant)
  }
}
