import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SparkSession, functions}

object FlatteningArray extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    Seq("a", "b", "c"),
    Seq("X", "Y", "Z")).toDF("value")

  //najpierw znajdź długość arraya, możemy to zrobić za pomocą metody size
  //  val array_size = input.withColumn("size", functions.size($"value"))

  val array_size = input.as[Seq[String]].head.size

  //w jaki sposób dodać kolumnę? withColumn
  //jak to zrobić? mając długość array i wykonać tyle razy withColumn? foldleft

  (0 until array_size).foldLeft(input) { (result, n) => result.withColumn(s"$n", $"value"(n)) }.drop("value").show

}
