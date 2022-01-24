package apache.berry.tutorial.spark.sql

import apache.berry.tutorial.spark.sql.Joins.DSJoin.{LeftJoinedRows, OuterJoinedRows}
import org.apache.spark.sql.{Column, Dataset, Encoder}

object DatasetHolder {
  implicit def toDatasetHolder[T](dataset: Dataset[T]): DatasetHolder[T] = {
    DatasetHolder(dataset)
  }

  case class InnerJoinedRows[A, B](left: A, right: B)

  case class DatasetHolder[T](dataset: Dataset[T]) {
    def leftOuterJoin[U](that: Dataset[U], condition: Column)(
      implicit
      jrEnc: Encoder[LeftJoinedRows[T, U]]
    ): Dataset[LeftJoinedRows[T, U]] = {
      dataset
        .joinWith(that, condition, "outer")
        .map {
          case (left, right) => LeftJoinedRows(left, Option(right))
        }
    }

    def OuterJoin[U](that: Dataset[U], condition: Column)(
      implicit
      jrEnc: Encoder[OuterJoinedRows[T, U]]
    ): Dataset[OuterJoinedRows[T, U]] = {
      dataset
        .joinWith(that, condition, "outer")
        .map { case (left, right) => OuterJoinedRows(Option(left), Option(right)) }
    }

  }
}
