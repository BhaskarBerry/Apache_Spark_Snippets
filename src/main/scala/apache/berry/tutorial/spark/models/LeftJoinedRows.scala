package apache.berry.tutorial.spark.models

case class LeftJoinedRows[A,B](left: A, right: Option[B])
