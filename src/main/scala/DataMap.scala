import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.expr

object DataMap extends App {
  val spark = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val nums = Seq(Seq(1, 2, 3)).toDF("nums")

  //weź seqa intów z columny value(r)
  //dla każdego elementwu a arrayu trzeba zrobić kolekcję par:
  val num = nums.flatMap { r =>
    val ns = r.getSeq[Int](0)
    ns.map(n => (ns, n))
  }

  //i teraz nazwij te kolumny:
  num.toDF("nums", "num").show()

}
