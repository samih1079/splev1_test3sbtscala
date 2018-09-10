case class MColordPrVD(var product:String,var m:Int=2,var mcBSD:MColorBSD) {

   var pv:Double=0;
   var sv:Double=0;
   var ov:Double=0;
   var psv:Double=0;

  def getPv=pv
  def getSv=sv
  def getOv=ov
  def getPsv=psv

  def compute(): Unit ={
    val g=mcBSD.subg;


//    pv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.para).edges.count()/klasses.toDouble;
//    sv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.subt).edges.count()/klasses.toDouble;
//    ov= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.over).edges.count()/klasses.toDouble;
//    psv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.para).edges.count()/klasses.toDouble;

  }


}
