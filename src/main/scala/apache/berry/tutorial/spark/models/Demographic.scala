package apache.berry.tutorial.spark.models

case class Demographic(
  id: Int,
  age: Int,
  codingBootCamp: Boolean,
  country: String,
  gender: String,
  isEthnicMinority: Boolean,
  servedInMilitary: Boolean
)
