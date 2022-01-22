package apache.berry.tutorial.spark.sql.datasources

import org.apache.spark.sql.{DataFrame, SaveMode}

object CSVSink {
  def save(
    dataFrame: DataFrame,
    intermediateLocation: String,
    saveMode: SaveMode = SaveMode.Overwrite,
    charset: String = "UTF-8",
    header: Boolean = false,
    delimiter: String = ","
  ): Unit = {
    dataFrame.write
      .option("header", header.toString)
      .option("charset", charset)
      .option("sep", delimiter)
      .option("escape", "\\")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .mode(saveMode)
      .csv(intermediateLocation)
  }

}
