import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{expr, lit}

object ImplicitDemo extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dept: DataFrame = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("values", "delimiter")

  //dynamiczne wyjęcie symboli z delimiter za pomocą expr
  val solution = dept.withColumn("split_values", expr("""split(values, concat('\\', delimiter))""")).show

}
