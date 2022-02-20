package apache.berry.tutorial.spark.datetime

import java.time.format.DateTimeFormatter
import java.time.ZoneId

object DateTimeFormatters {
  private val patternsMap = scala.collection.mutable.Map[DateTimeFormatter, String]()

  //JST zoned Formatters
  val jst_yyyyMM: DateTimeFormatter = fromPatternWithZone("yyyyMM", TimeZones.JST)

  def patternOf(dateTimeFormatter: DateTimeFormatter): String = {
    patternsMap.getOrElse(dateTimeFormatter, {
      throw new RuntimeException(s"Unknown formatter : $dateTimeFormatter")
    })
  }

  private def fromPatternWithZone(pattern: String, zoneId: ZoneId) = {
    val formatter = DateTimeFormatter.ofPattern(pattern).withZone(zoneId)
    patternsMap.put(formatter, pattern)
    formatter
  }
}
