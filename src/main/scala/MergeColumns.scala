import org.apache.spark.sql.SparkSession

object MergeColumns extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = Seq(
    ("100", "John", Some(35), None),
    ("100", "John", None, Some("Georgia")),
    ("101", "Mike", Some(25), None),
    ("101", "Mike", None, Some("New York")),
    ("103", "Mary", Some(22), None),
    ("103", "Mary", None, Some("Texas")),
    ("104", "Smith", Some(25), None),
    ("105", "Jake", None, Some("Florida"))).toDF("id", "name", "age", "city")

  val mergeCol = input.groupBy("id", "name")
    .agg(first("age", ignoreNulls = true) as "age", first("city", ignoreNulls = true) as "city")
    .orderBy("id")

  mergeCol.show()
}
