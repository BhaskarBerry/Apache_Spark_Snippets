package apache.berry.tutorial.spark.datetime

import java.time.ZoneId

object TimeZones {
  val JST: ZoneId = ZoneId.of("Asia/Tokyo")
  val UTC: ZoneId = ZoneId.of("UTC")
  val IST: ZoneId = ZoneId.of("Asia/Kolkata")
}
