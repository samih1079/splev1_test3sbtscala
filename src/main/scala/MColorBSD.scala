import org.apache.spark.graphx.Graph

import scala.collection.mutable.{Set => MutableSet}

case class MColorBSD(var subg: Graph[(String, String), String]) extends Ordered[MColorBSD] {

  var ps: Double = 0
  var ss: Double = 0
  var os: Double = 0
  var ns: Double = 0
  var m_color: Long = 0

  def copyGraph(subgPrm: Graph[(String, String), String]): Unit = {
    subg = subgPrm
    compute()
  }

  def compute(): Unit = {
    var progsSet: Set[String] = Set()

    var cps = subg.edges.filter(e => e.attr == SimTypeMames.para).count();
    var cos = subg.edges.filter(e => e.attr == SimTypeMames.over).count();
    var css = subg.edges.filter(e => e.attr == SimTypeMames.subt).count()
    var cns = subg.edges.filter(e => e.attr == SimTypeMames.non).count()
    subg.vertices.collect().foreach(v => {
      if (!progsSet.contains(v._2._1))
        progsSet += v._2._1
    });
    m_color = progsSet.size
    val k = subg.vertices.cache().count()
    ps = (2.0 * cps) / (k * (k - 1))
    os = (2.0 * cos) / (k * (k - 1))
    ss = (2.0 * css) / (k * (k - 1))
    ns = (2.0 * cns) / (k * (k - 1))
  }

  override def toString: String = "MColorBSD:" + getSubGrpaphEdges() + '\n' +
    "m:" + m_color + " ps:" + ps + " os:" + os + " ss:" + ss + " ns:" + ns


  def getSubGrpaphEdges(): String = {
    var res = ""
    subg.edges.collect().foreach(e => res += e.toString)
    res
  }

  override def compare(that: MColorBSD): Int = {
    var res: Int = 0;
    if (this.ps < that.ps) res = 1
    else if (this.ps == that.ps && this.ss < that.ss) res = 1
    else if (this.ps == that.ps && this.ss == that.ss && this.os < that.os) res = 1
    else
      res = (-1)
    res
  }
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