import org.apache.spark.graphx.Graph

import scala.collection.mutable.{Set => MutableSet}

case class MColorBSD (var subg:Graph[(String,String),String]){

  var ps:Double=0
  var ss:Double=0
  var os:Double=0
  var ns: Double=0
  var m_color:Long=0

  def copyGraph(subgPrm:Graph[(String,String),String]): Unit ={
    subg=subgPrm
    compute()
  }
  def compute(): Unit ={
    var progsSet:Set[String]=Set()

    subg.vertices.cache().foreach(v=>{
      if(!progsSet.contains(v._2._1))
      progsSet+=v._2._1
    });
    m_color=progsSet.size
    var cps=0 var cos=0 var css=0 var cns=0
    subg.edges.foreach(edge=> {
      if (edge.attr == "parametric") cps += 1
      if (edge.attr == "overloading") cos += 1
      if (edge.attr == "subtyping") css += 1
      if (edge.attr == "none") cns += 1
    })
    val k=subg.vertices.cache().count();
    ps=(2*cps)/(k*(k-1))
    os=(2*cos)/(k*(k-1))
    ss=(2*css)/(k*(k-1))
    ns=(2*cns)/(k*(k-1))
  }






}
