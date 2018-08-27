import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import java.io.IOException

import SPLETools.{spark, _}
import org.apache.spark.sql.DataFrame
object SimilarityDegree extends App {
  val spark =intitSpark()
val resChema="prog1 STRING,prog2 STRING,class1 STRING,class2 STRING,use INT,over INT,ext INT,ref INT,ref_ext INT,parametric DOUBLE,subtyping DOUBLE,overloading DOUBLE,total DOUBLE,type STRING"


  val relationRes=readRes("output"+File.separator +"spleres.csv",resChema)


}
