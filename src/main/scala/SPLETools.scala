import java.io.{File, IOException}
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.util.Try



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
     var outputDir="\\output\\result"
     for (i <- 1 to iterations) {
       println(s"iter:$i")
       val tmpDf = progs.limit(batchSize)
       val tmpVals = tmpDf.collect()
       tmpVals.foreach(r => {
         println(r.getString(0) + " " + r.getString(1))
         val towProgsRes = classesResEachProgPair(r.getString(0),r.getString(1),allClassRes,0.8,0.8,0.8)
          towProgsRes.show()

         //todo save to single csv file
         //saveDfToCsv(towProgsRes,"result"+".csv")
        towProgsRes.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(outputDir)

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
     val dir = new File(outputDir)
     val resultname="spleres.csv"
//     var resfile:File=new File(dir+File.separator+resultname)
     dir.listFiles().foreach(f=>{
       val fname=f.getName
       if ((fname.startsWith("part-00000")&& fname.endsWith(".csv") ))
         {}
       else
         f.delete();
     })
     val check=merge(outputDir,"\\output"+File.separator+resultname,true)
     //resfile.renameTo(new File("res.csv"))
   }

   def classesResEachProgPair(prog1:String,prog2:String, allClassRes:DataFrame,paraThresh:Double,subThresh:Double,overThresh:Double):DataFrame={
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
       .withColumn("type",
         when(col("parametric") >= paraThresh,"parametric")
           .when(col("subtyping")>=subThresh,"subtyping")
           .when(col("overloading")>=subThresh,"overloading").otherwise("none"))
   }


   def merge(srcPath: String, dstPath: String, delSource:Boolean): Unit =  {
     val hadoopConfig = new Configuration()
     val hdfs = FileSystem.get(hadoopConfig)
     FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), delSource, hadoopConfig, null)
     // the "true" setting deletes the source files once they are merged into the new output
   }

   def copyMerge(
                  srcFS: FileSystem, srcDir: Path,
                  dstFS: FileSystem, dstFile: Path,
                  deleteSource: Boolean, conf: Configuration
                ): Boolean = {

     if (dstFS.exists(dstFile))
       throw new IOException(s"Target $dstFile already exists")

     // Source path is expected to be a directory:
     if (srcFS.getFileStatus(srcDir).isDirectory()) {

       val outputFile = dstFS.create(dstFile)
       Try {
         srcFS
           .listStatus(srcDir)
           .sortBy(_.getPath.getName)
           .collect {
             case status if status.isFile() =>
               val inputFile = srcFS.open(status.getPath())
               Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
               inputFile.close()
           }
       }
       outputFile.close()

       if (deleteSource) srcFS.delete(srcDir, true) else true
     }
     else false
   }
//   def saveDfToCsv(df: DataFrame, tsvOutput: String): Unit = {
//     val tmpParquetDir = "s_output"
////         towProgsRes.coalesce(1).
//     // write.format("com.databricks.spark.csv").
//     // option("header", "true")
//     // .mode("append").save("\\output\\result"+".csv")
//     df.repartition(1).write.
//       format("com.databricks.spark.csv").
//       option("header", "true").
//       save(tmpParquetDir)
//
//     val dir = new File(tmpParquetDir)
//     val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
//     (new File(tmpTsvFile)).renameTo(new File(tsvOutput))
//
//     dir.listFiles.foreach( f => {
//       f.delete
//     } )
//
//     dir.delete
//   }


//   def func(df: DataFrame, tsvOutput: String): Unit ={
  // write csv into temp directory which contains the additional spark output files
  // could use Files.createTempDirectory instead
//  val file = new File(tsvOutput)
//  val tempDir = file.getParent;
//  df.coalesce(1)
//    .write.format("com.databricks.spark.csv")
//    .option("header", "true")
//    .save(file.getName)
//
//  // find the actual csv file
//  val tmpCsvFile = Files.walk(, 1).iterator().toSeq.find { p =>
//    val fname = p.getFileName.toString
//    fname.startsWith("part-00000") && fname.endsWith(".csv") && Files.isRegularFile(p)
//  }.get
//
//  // move to desired final path
//  Files.move(tmpCsvFile, file)
//
//  // delete temp directory
//  Files.walk(tempDir)
//    .sorted(java.util.Comparator.reverseOrder())
//    .iterator().toSeq
//    .foreach(Files.delete(_))
//}


}
