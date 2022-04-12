import org.apache.spark.sql.SparkSession

object Prefix extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val input = Seq(
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

  val countOccurences = udf { (ss: Seq[String]) => ss.groupBy(identity).mapValues(_.size).maxBy(_._2) }

  val solution = input.where($"PREFIX".isNotNull)
    .groupBy($"UNIQUE_GUEST_ID")
    .agg(collect_list($"PREFIX") as "LIST")
    .withColumn("Occ", countOccurences($"LIST")).drop($"LIST")

  solution.show

}