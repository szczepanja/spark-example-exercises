import org.apache.spark.sql.{SparkSession, functions}

object FindingIds extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val w = Seq(
    (1, "one,two,three", "one"),
    (2, "four,one,five", "six"),
    (3, "seven,nine,one,two", "eight"),
    (4, "two,three,five", "five"),
    (5, "six,five,one", "seven")).toDF("id", "words", "word")

  w.show

}