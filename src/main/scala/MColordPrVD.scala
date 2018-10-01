case class MColordPrVD(var product:String,var m:Int=2) {

   var pv:Double=0;
   var sv:Double=0;
   var ov:Double=0;
   var psv:Double=0;
  var k:Int=0
   var graph4vlass:Map[String,MColorBSD]=Map()


  override def toString: String = "MColordPrVD:"+product+'\n'+"k:"+k+ ",pv:"+pv+",sv:"+sv+",ov:"+ov+",psv:"+psv+'\n'+
  getStringGraphs()


  def getStringGraphs():String={
   var res:String="getStringGraphs size:"+graph4vlass.size+"  "

    graph4vlass.toSeq.sortBy(_._2).foreach(f=>res+="class:"+f._1+":"+f._2.toString+'\n')
  res
  }
//  def getPv=pv
//  def getSv=sv
//  def getOv=ov
//  def getPsv=psv




//    pv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.para).edges.count()/klasses.toDouble;
//    sv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.subt).edges.count()/klasses.toDouble;
//    ov= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.over).edges.count()/klasses.toDouble;
//    psv= g.subgraph(epred = e=> e.srcAttr._1== product && e.attr==SimTypeMames.para).edges.count()/klasses.toDouble;
}
