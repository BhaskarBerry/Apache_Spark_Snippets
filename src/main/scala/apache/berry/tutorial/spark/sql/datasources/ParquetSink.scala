package apache.berry.tutorial.spark.sql.datasources

import org.apache.spark.sql.{Dataset, SaveMode}

object ParquetSink {

  def save[T](targetPath: String, dataset:Dataset[T], saveMode: SaveMode): Unit={
    dataset
      .toDF()
      .write
      .mode(saveMode)
      .parquet(targetPath)
  }

}
