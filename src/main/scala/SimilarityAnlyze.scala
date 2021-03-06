import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import SPLETools._

object SimilarityAnlyze extends App {
  val spark =intitSpark()

  var sourceSchema="method1 STRING,class1	STRING,program1	STRING,method2	STRING,class2	STRING,program2	STRING," +
    "params1 STRING,params2 STRING,return1 STRING,return2 STRING,used1 STRING,used2	STRING," +
    "mod1	STRING,mod2	STRING,shallow	STRING,deep	STRING,result STRING"

  val sourceCsv:DataFrame =read("debugInfo_1533200012911fxd.csv")
  //val sourceCsv:DataFrame =readWithSchema("debugInfo_1533200012911.csv",sourceSchema)

  // println(peopleDFCsv.schema)
  //peopleDFCsv.columns.foreach(c=>println(c))
  import spark.implicits._
  val res=countResultNoNullForEachProg(sourceCsv)

  showClassesResultForEachProgPair(res)

  // SparkSession.builder().master("local").appName("test1").getOrCreate();
  //spark.sparkContext.setLogLevel("ERROR")
//  val peopleDFCsv = spark
//    .read
//    .format("csv")
//    .option("sep", ",")
//    .option("inferSchema", "true")
//    .option("header", "true")
//    .load("debugInfo_1533200012911fxd.csv").toDF()


//  val res = peopleDFCsv
//    //.filter("result != 'NULL'")
//    .filter(col("result") =!= ("NULL"))
//    .orderBy("program1", "program2", "class1", "class2")
//    .groupBy("program1", "program2", "class1", "class2", "result")
//    .agg(count("result"))



//  private var progs: Dataset[Row] = res.select("program1", "program2")
//    .orderBy("program1", "program2").distinct()
//  var progs:DataFrame=allProgsDistinctPairs(res)
//  val progsCount = progs.count()
//  val batchSize = 10
//  val iterations = Math.max(Math.ceil(progsCount / batchSize) , 1).intValue()
//
//  for (i <- 1 to iterations) {
//    println(s"iter:$i")
//    val tmpDf = progs.limit(batchSize)
//    val tmpVals = tmpDf.collect()
//    tmpVals.foreach(r => {
//      println(r.getString(0) + " " + r.getString(1))
//      val classes = res
//        .select("*")
//        .filter("program1='" + r.getString(0) + "'" + " AND program2='" + r.getString(1) + "'")
//      classes.withColumn("use", when($"result" === "USE", $"count(result)").otherwise(0))
//        .withColumn("overloading", when($"result" === "OVERLOADING", $"count(result)").otherwise(0))
//        .withColumn("extension", when($"result" === "EXTENSION", $"count(result)").otherwise(0))
//        .withColumn("refine_extension", when($"result" === "REFINED_EXTENSION", $"count(result)").otherwise(0))
//        .groupBy("program1", "program2", "class1", "class2")
//        .sum("use", "overloading", "extension","refine_extension")
//        .show()
//
//    })
//    progs = progs.except(tmpDf)
//  }

  //res.show(100);
  // private val value: Dataset[Row] = res.select("count(result)").filter("program1='" + "P14" + "'" + " AND program2='" + "P15" + "'")
  // value.show()
  //  progs.foreach(row=>{
  //    println(row.getString(0)+" "+row.getString(1))
  //  })
  //  private val progs: Dataset[Row] = res.select("program1","program2").orderBy("program1","program2").distinct()
  //  private val testp14p15: Dataset[Row] = res.select("*").filter("program1='"+"P14"+"'" + " AND program2='"+"P15"+"'")
  //  testp14p15
  //    .withColumn("use",when($"result" === "USE",$"count(result)").otherwise(0))
  //    .withColumn("overloading",when($"result" === "OVERLOADING",$"count(result)").otherwise(0))
  //    .withColumn("extension",when($"result" === "EXTENSION",$"count(result)").otherwise(0))
  //    .groupBy("program1","program2","class1","class2")
  //    .sum("use","overloading","extension")
  //    .show(100)
  //



  //  print("hiii")
  //res.show(200);


}
