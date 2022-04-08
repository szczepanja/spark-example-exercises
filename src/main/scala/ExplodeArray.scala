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
}
