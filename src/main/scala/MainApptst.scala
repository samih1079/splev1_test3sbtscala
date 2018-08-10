import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
object MainApptst extends App {
  print("jjjj")
  val spark = SparkSession.builder().master("local").appName("test1").getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")
  val peopleDFCsv = spark.read.
    format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("debugInfo_1533200012911fxd.csv").toDF()

  peopleDFCsv.createOrReplaceTempView("progs")

  import spark.implicits._
  val qeury="select count(result),class1,class2 from progs group by (class1,class2)"

  val res=peopleDFCsv.groupBy("class1","class2","result","program1","program2").agg(count("result"))
  res.show()


}
