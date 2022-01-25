package apache.berry.tutorial.spark.models

case class Finances(
  id: Int,
  hasDebt: Boolean,
  hasFinancialDependents: Boolean,
  hasStudentLoans: Boolean,
  income: Int
)
