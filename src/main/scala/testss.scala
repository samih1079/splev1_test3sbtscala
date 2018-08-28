import org.apache.spark.sql.catalyst.expressions.Murmur3Hash

import scala.util.MurmurHash
import scala.util.hashing.MurmurHash3

object testss extends App {

  //var h:Murmur3Hash=new
  println(MurmurHash3.stringHash("abc"))
  println(MurmurHash3.stringHash("abc"))

  println(MurmurHash3.stringHash("abc"))


}
