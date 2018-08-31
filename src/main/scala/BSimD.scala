import scala.collection.mutable.{ Set => MutableSet }

case class BSimD (var ps:Double=0,var ss:Double=0,var os:Double=0,var clique:MutableSet[Long]){

  def psv()=ps
  def ssv()=ss
  def osv()=os
  def grVerSet()=clique


}
