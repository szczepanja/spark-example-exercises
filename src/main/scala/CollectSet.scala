import org.apache.spark.sql.SparkSession

object CollectSet {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._
  import org.apache.spark.sql.functions._

  val input = spark.range(50).withColumn("key", $"id" % 5)

  val groupInput = input.groupBy("key").agg(collect_set("id") as "all")

  val solution = groupInput.withColumn("three_only", slice('all, 1, 3))

  solution.show

}
