import org.apache.spark.sql.SparkSession

object ExplodeArray extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val nums = Seq(Seq(1, 2, 3)).toDF("nums")

  val num = nums.withColumn("num", explode($"nums"))

  num.show()

  val dates = Seq(
    ("08-11-2015"),
    ("09-11-2015"),
    ("09-12-2015")).toDF("date")

  val toDate = dates.select($"date_string", to_date($"date_string", "dd/MM/yyyy"))
  toDate.withColumn("diff", datediff(current_date(), $"to_date(date_string, dd/MM/yyyy)")).show

}
