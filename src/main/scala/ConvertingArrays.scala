import org.apache.spark.sql.SparkSession

object ConvertingArrays extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val words = Seq(Array("hello", "world")).toDF("words")

  val solution = words.withColumn("solution", concat_ws(" ", $"words"))

  solution.show()

}
