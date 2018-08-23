import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


 object SPLETools {
   var spark:SparkSession = SparkSession.builder().master("local").appName("test1").getOrCreate()

   def intitSpark():SparkSession= {
    spark
   }


  def read(csvFile:String): DataFrame =
  {
    spark.sparkContext.setLogLevel("ERROR")
    val peopleDFCsv = spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile).toDF()
    peopleDFCsv
  }
   def countResultNoNullForEachProg(dataFrame: DataFrame):DataFrame =
   {
     dataFrame
     //.filter("result != 'NULL'")
     .filter(col("result") =!= ("NULL"))
     .orderBy("program1", "program2", "class1", "class2")
     .groupBy("program1", "program2", "class1", "class2", "result")
     .agg(count("result"))
   }
   def allProgsDistinctPairs(dataFrame: DataFrame): DataFrame =
   {
     dataFrame.select("program1", "program2")
       .orderBy("program1", "program2").distinct()
   }

   def showClassesResultForEachProgPair(allClassRes:DataFrame): Unit ={
     var progs=allProgsDistinctPairs(allClassRes);
     val progsCount = progs.count()
     val batchSize = 10
     val iterations = Math.max(Math.ceil(progsCount / batchSize) , 1).intValue()

     for (i <- 1 to iterations) {
       println(s"iter:$i")
       val tmpDf = progs.limit(batchSize)
       val tmpVals = tmpDf.collect()
       tmpVals.foreach(r => {
         println(r.getString(0) + " " + r.getString(1))
         classesResEachProgPair(r.getString(0),r.getString(1),allClassRes).show();
//         val classes = allClassRes
//           .select("*")
//           .filter("program1='" + r.getString(0) + "'" + " AND program2='" + r.getString(1) + "'")
//         classes.withColumn("use", when(col("result") === "USE", col("count(result)")).otherwise(0))
//           .withColumn("overloading", when(col("result") === "OVERLOADING", col("count(result)")).otherwise(0))
//           .withColumn("extension", when(col("result") === "EXTENSION", col("count(result)")).otherwise(0))
//           .withColumn("refine_extension", when(col("result") === "REFINED_EXTENSION", col("count(result)")).otherwise(0))
//           .groupBy("program1", "program2", "class1", "class2")
//           .sum("use", "overloading", "extension","refine_extension")
//           .show()

       })
       progs = progs.except(tmpDf)
     }
   }

   def classesResEachProgPair(prog1:String,prog2:String, allClassRes:DataFrame):DataFrame={
     allClassRes
       .select("*")
       .filter("program1='" + prog1 + "'" + " AND program2='" + prog2 + "'")
       .withColumn("use", when(col("result") === "USE", col("count(result)")).otherwise(0))
       .withColumn("overloading", when(col("result") === "OVERLOADING", col("count(result)")).otherwise(0))
       .withColumn("extension", when(col("result") === "EXTENSION", col("count(result)")).otherwise(0))
       .withColumn("refinement", when(col("result") === "REFINEMENT", col("count(result)")).otherwise(0))
       .withColumn("refine_extension", when(col("result") === "REFINED_EXTENSION", col("count(result)")).otherwise(0))
       .groupBy("program1", "program2", "class1", "class2")
       .sum("use", "overloading", "extension","refinement","refine_extension")
       .withColumn("parametric",
         col("sum(use)")/(col("sum(use)")+col("sum(overloading)")+col("sum(extension)")+col("sum(refinement)")+col("sum(refine_extension)")))
       .withColumn("subtyping",
         (col("sum(extension)")+col("sum(refinement)")+col("sum(refine_extension)"))/(col("sum(use)")+col("sum(overloading)")+col("sum(extension)")+col("sum(refinement)")+col("sum(refine_extension)")))
       .withColumn("overloading",
         col("sum(overloading)")/(col("sum(use)")+col("sum(overloading)")+col("sum(extension)")+col("sum(refinement)")+col("sum(refine_extension)")))
       .withColumn("total",
         (col("sum(use)")+col("sum(overloading)")+col("sum(extension)")+col("sum(refinement)")+col("sum(refine_extension)")))

   }




}
