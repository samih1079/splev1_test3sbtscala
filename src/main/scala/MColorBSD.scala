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



    var cps=subg.edges.filter(e=> e.attr=="parametric").count();
    var cos=subg.edges.filter(e=> e.attr=="overloading").count();
    var css=subg.edges.filter(e=> e.attr=="subtyping").count()
    var cns=subg.edges.filter(e=> e.attr=="none").count()
    subg.vertices.collect().foreach(v=>{
      if(!progsSet.contains(v._2._1))
        progsSet+=v._2._1
    });
    m_color=progsSet.size
    val k=subg.vertices.cache().count()
    ps=(2.0*cps)/(k*(k-1))
    os=(2.0*cos)/(k*(k-1))
    ss=(2.0*css)/(k*(k-1))
    ns=(2.0*cns)/(k*(k-1))
  }
  override def toString: String = "m:"+m_color+" ps:"+ps+" os:"+os+" ss:"+ss+" ns:"+ns
}
//        subg.edges.collect().foreach(edge=> {
//      //println(edge+"   attr:"+edge.attr)
//      if (edge.attr == "parametric"){ cps +=1
//      //  println(edge+"   cps:"+cps)
//      }
//      if (edge.attr == "overloading") cos+=1
//      if (edge.attr == "subtyping") css += 1
//      if (edge.attr == "none") cns += 1
//    })