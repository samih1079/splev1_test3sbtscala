import java.io.File

import org.apache.spark._
import org.apache.spark.graphx
import org.apache.spark.rdd.RDD
import SPLETools.{spark, _}
import org.apache.spark.graphx._
import MyGraphNdSetUtils._
import SimTypeMames._
import org.apache.spark.sql.DataFrame

import scala.util.MurmurHash
import scala.collection.mutable.{ Set => MutableSet }
import scala.util.hashing.MurmurHash3
object SimilarityDegree extends App {
//  val spark =intitSpark()



  //in order to read the result file contaontin relation type: "output/spleres.csv"
  val resChema="prog1 STRING,prog2 STRING,class1 STRING,class2 STRING,use INT,over INT,ext INT,ref INT,ref_ext INT,parametric DOUBLE,subtyping DOUBLE,overloading DOUBLE,total DOUBLE,type STRING"

  // Assume the SparkContext has already been constructed

  val fullGraph=buildRelationGraph();
//  val vc=fullGraph.vertices.filter{ case (id,(prog,class1))=>prog == "P3" || prog=="p6"}.count()
//  println(vc)
  //to visualization
//  val pw = new java.io.PrintWriter("output/myGraph.gexf")
//  pw.write(toGexf(fullGraph))
//  pw.close
val conf=new SparkConf().setAppName("graphtest1").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
  //val sc: SparkContext=new SparkContext(conf)
  val sc = SparkContext.getOrCreate()
  sc.setLogLevel("ERROR")//val bk = new BronKerboschSCALA(sc, fullGraph).runAlgorithm;
//
//  bk.foreach { println }

//test visual Graph
  val subg=fullGraph.subgraph(epred = e=>e.attr!=non && e.srcAttr._1=="P3" && e.dstAttr._1=="P6")
  println(subg.vertices.count())
 // println(subg.edges.count())
  saveGexf(subg)
getMcolorPrVDSetByForProgram(fullGraph,"P3",2)

//  var vset:Set[Long] = Set[Long]()
//  //
//  subg.vertices.collect().foreach(v=> {
//    vset=vset+v._1
//    //  print(v._1+",")
//  })
//  println("size:"+vset.size)
//  // println()
//  var allPotinalGraph:Set[MColorBSD]=Set()
//  MyGraphNdSetUtils.power3(vset, 2).foreach(u=>{
//    println("U:"+u)
//    val sub=subg.subgraph(vpred = (id,attr)=>u.contains(id)).cache()
//    if(MyGraphNdSetUtils.isConnectedGraph(sub))
//    {
//      print("wait.")
//      val tmp:MColorBSD=new MColorBSD(sub);
//      tmp.compute()
//      allPotinalGraph+=tmp;
//    }
//  })
//  println("allPotinalGraph:"+allPotinalGraph.size)
//  allPotinalGraph.foreach(s=>{
//    s.subg.edges.foreach(e=> println(e))
//    println(s)
//  })
//


  /**
    * compute all M-color for each class in Prog
    * @param graph full graph with egdes's attr:para sub over
    * @param pro program name
    * @param m  minimal number of related program
    * @return a set of McoloBSD conatainas all metreics includes the sub graph
    */
  def getMColorForPRog(graph: Graph[(String,String),String], pro:String,m:Int):Set[MColorBSD]={
    var mset:Set[MColorBSD]=Set()
    //var cset:Map[Long,String]=Map()
   // println("getMColorForPRog cset:")
    graph.vertices.collect().filter { case (id, (prog, klass)) => prog == pro }foreach(v=>{
      //cset+= (v._1->v._2._2)
      println(pro+":"+v._2._2)
      val ccg=getComponentByVr(graph,v._1)
      val tmp:MColorBSD=new MColorBSD(ccg);
      tmp.compute()
      println("getMColorForPRog:"+tmp)
      if(tmp.m_color>=m)
      {
        mset+=tmp
      }
    })
//    cset.foreach(s=>{
//
//    })
   // println("getMColorForPRog:"+mset.size)
    mset
  }

  def getMcolorPrVDSetByForProgram(graph: Graph[(String,String),String], pro:String,m:Int):MColordPrVD={
    //val mcSet:Set[MColorBSD]=getMColorForPRog(graph,pro,m)

    var prdv:MColordPrVD=MColordPrVD(pro,m)

    var cpv:Long=0;var csv:Long=0;var cov:Long=0;var cpsv:Long=0;var k:Int=0;
    //get all program class's
    graph.vertices.collect().filter { case (id, (prog, klass)) => prog == pro }foreach(v=>{
      //cset+= (v._1->v._2._2)
      //println("getMcolorPrVDSetByForProgram:"+pro+":"+v._2._2)
      val ccg=getComponentByVr(graph,v._1)
      val tmp:MColorBSD=new MColorBSD(ccg);
      tmp.compute()
      //println(tmp)
      k+=1
      if(tmp.m_color>=m)
      {
        prdv.graph4vlass+=(v._2._2->tmp)
        val cpara= ccg.subgraph(epred = e=> (e.srcAttr._1== pro && e.srcAttr._2== v._2._2 || e.dstAttr._1== pro && e.dstAttr._2== v._2._2) && e.attr==SimTypeMames.para).edges.count()
        cpv+=cpara
       val csub= ccg.subgraph(epred = e=> (e.srcAttr._1== pro && e.srcAttr._2== v._2._2 || e.dstAttr._1== pro && e.dstAttr._2== v._2._2) && e.attr==SimTypeMames.subt).edges.count()
        csv+=csub
        var cover= ccg.subgraph(epred = e=> (e.srcAttr._1== pro && e.srcAttr._2== v._2._2 || e.dstAttr._1== pro && e.dstAttr._2== v._2._2) && e.attr==SimTypeMames.over).edges.count()
        cov+=cover
        if(tmp.m_color==1 || (cpara==0 && csub==0 && cover==0))
            cpsv+=1
      }
      else
      if(tmp.m_color==1)
        cpsv+=1
    })
    prdv.k=k
    prdv.ov=cov/k.toDouble
    prdv.sv=csv/k.toDouble
    prdv.pv=cpv/k.toDouble
    prdv.psv=cpsv/k.toDouble
    println("getMcolorPrVDSetByForProgram:\n"+prdv)
    prdv
  }

  def buildRelationGraph(): Graph[(String, String), String] = {
    val relationRes = readWithSchema("output/spleres.csv", resChema)
    val resForEdges = relationRes.select("prog1", "class1", "prog2", "class2", "type").rdd;
    //val edges:RDD[(VertexId,VertexId)]=res.map(r=>(MurmurHash3.stringHash(r.getString(0))  ,MurmurHash3.stringHash(r.getString(1))))
    //edge: p1class1--[type]--p2class2
    val edges: RDD[Edge[String]] = resForEdges.map(
      r => Edge(MurmurHash3.stringHash(r.getString(0) + "_" + r.getString(1)), //fo example hashing : p1_Model.Answer
        MurmurHash3.stringHash(r.getString(2) + "_" + r.getString(3)),
        r.getString(4)))
    //vertex is: program and class together
    //first we union all progs and class under prog1 and class1
    val progclassUnion = relationRes.select("prog1", "class1").union(relationRes.select("prog2", "class2"))
    //
    val vert: RDD[(VertexId, (String, String))] = progclassUnion.rdd.map(
      r => (MurmurHash3.stringHash(r.getString(0) + "_" + r.getString(1)), //fo example hashing : p1_Model.Answer
        (r.getString(0), //fist label is the PROGRAM name
          r.getString(1))))
    //second label is the program name
    val defaultUser = ("John Doe", "Missing")
    //
    //  // Build the initial MyGraph
    // val g2=MyGraph.fromEdges(edges,"");
    val graph = Graph(vert, edges, defaultUser)
    graph
  }



  def toGexf[VD,ED](g:Graph[VD,ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"

    def saveGexf[VD,ED](graph:Graph[VD,ED]): Unit ={
        val pw = new java.io.PrintWriter("output/myGraph.gexf")
        pw.write(toGexf(graph))
        pw.close
    }

  //  val vc=graph.vertices.filter{ case (id,(name,pos))=>pos == "prof"}.count()
  //val graph= MyGraph.fromEdgeTuples(edges,1);

  //graph.triplets.collect.foreach(println)

//  val users: RDD[(VertexId, (String, String))] =sc.parallelize(Array((0, ("istoica", "prof")),
//      (3L, ("rxin", "student")),
//      (4L, ("franklin", "prof")),
//      (7L, ("jgonzal", "postdoc"))))
//  // Create an RDD for edges
//  val relationships: RDD[Edge[String]] =
//    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
//      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
//  // Define a default user in case there are relationship with missing user
//  val defaultUser = ("John Doe", "Missing")
//
//  // Build the initial MyGraph
//  val graph = MyGraph(users, relationships, defaultUser)
//  val vc=graph.vertices.filter{ case (id,(name,pos))=>pos == "prof"}.count()
//  val ec=graph.edges.filter(e=>e.srcId>e.dstId).count()
//  val facts:RDD[String]=graph.triplets.map(triple=>triple.srcAttr._1+" is the "+triple.attr+ "of "+triple.dstAttr._1)
//  facts.collect().foreach(println(_))
//  println("Vs:"+vc+" Es:"+ec)

}
