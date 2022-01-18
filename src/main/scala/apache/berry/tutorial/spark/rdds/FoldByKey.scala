package apache.berry.tutorial.spark.rdds

/**
 * 1. def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
 *
 * 2. def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)]
 *
 * Merge the values for each key using an associative function and a neutral "zero value" which
 * may be added to the result an arbitrary number of times, and must not change the result
 * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
 *
 * foldByKey should be used in use cases where values need to be aggregated based on keys's.
 *
 */

object FoldByKey extends App {
  val empData = sc.parallelize(List(
    ("BD Engineer", ("Bose", 100000, 9)),
    ("BD Engineer", ("Berry", 120000, 8)),
    ("Python Dev", ("Ayub", 190000, 5)),
    ("Power BI", ("Pavani", 200000, 2)),
    ("Power BI", ("Mohan", 200000, 5)),
    ("ML Engineer", ("Bhaskar", 800000, 9)),
    ("ML Engineer", ("Rose", 500000, 4)),
    ("AI Engineer", ("Bhaskar", 600000, 3))
  ))

  val maxSalByProfile = empData.foldByKey(("Default",0,0)) ((acc, elem) => {
    if(acc._2 > elem._2) acc else elem
  })

  println("Maximum salaries in each profile = "+ maxSalByProfile.collect().toList)

  /*
   Maximum salaries in each profile = List(
   (Python Dev,(Ayub,190000,5)),
   (Power BI,(Mohan,200000,5)),
   (BD Engineer,(Berry,120000,8)),
   (ML Engineer,(Bhaskar,800000,9)),
   (AI Engineer,(Bhaskar,600000,3)))
   */

}
