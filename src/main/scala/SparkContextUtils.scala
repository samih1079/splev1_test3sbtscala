import java.net.InetAddress
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextUtils {

  val conf=new SparkConf().setAppName("graphtest1").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
  val sc: SparkContext=new SparkContext(conf)
  sc.setLogLevel("ERROR")



  def getSparkContext: SparkContext = {
    sc
  }
}