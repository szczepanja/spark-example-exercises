import org.apache.spark.sql.{SparkSession, functions}
//import spark.implicits._

object RollupOperator extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  val salaries = spark.read.option("header", value = true)
    .option("inferSchema", value = true)
    .csv("/mnt/c/Users/a.szczepanik/Projekty/spark-split/salaries.csv")

  val average = salaries.rollup("department", "salary").avg()
}