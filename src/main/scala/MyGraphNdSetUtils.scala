import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
object MyGraphNdSetUtils {
  def powerByLength[T](set: Set[T], length: Int) = {
    var res = Set[Set[T]]()
    res ++= set.map(Set(_))

    for (i <- 1 until length)
      res = res.map(x => set.map(x + _)).flatten

    res
  }

  def power[A](t: Set[A]): Set[Set[A]] = {
       @annotation.tailrec
       def pwr(t: Set[A], ps: Set[Set[A]]): Set[Set[A]] =
            if (t.isEmpty) ps
         else pwr(t.tail, ps ++ (ps map (_ + t.head)))

         pwr(t, Set(Set.empty[A])) //Powerset of ∅ is {∅}
       }

  //def powerSet5[A](xs: List[A]) = xs filterM (_ => true :: false :: Nil)

  def power3[T](xs:Set[T],lenth:Int):Set[Set[T]]= {
    val resset=(lenth to xs.size).flatMap(xs.toSeq.combinations).map(_.toSet).toSet
    resset
  }
  def powerSet2 (l:List[_]) : List[List[Any]] =
    l match {
      case Nil => List(List())
      case x::xs =>
        var a = powerSet2(xs)
        a.map(n => n:::List(x)):::a
    }
  def powerSet3[A](xs: Seq[A]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq[A]())) {(sets, set) => sets ++ sets.map(_ :+ set)}

  /**
    * Represent breadth-first search statement of social graph
    * via delegation to Pregel algorithm starting from the edge root
    * @param root The point of departure in BFS
    * @return breadth-first search statement
    */
   def getBFS(root:VertexId,graph: Graph[(String,String),String]) = {
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    val bfs = initialGraph.pregel(Double.PositiveInfinity, 1)(
      (_, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr+1))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b)).cache()
    bfs
  }

  def isConnectedGraph(graph:Graph[(String,String),String]): Boolean={

    if(graph.edges.count()==0) return false
//    println("edgs")
//    graph.edges.foreach(e=>print(e))
//    println()
//    println("verts")
//    graph.vertices.foreach(e=>print(e))
//    println()
     val neighbourVerticesMap = graph.collectNeighborIds(EdgeDirection.Either)
      .collect().map(vertex => (vertex._1.asInstanceOf[Long], vertex._2.toSet))
      .toMap;
    //neighbourVerticesMap.foreach(v=>println("NeighborIds:"+v))
    var q:ListBuffer[Long]=ListBuffer();
    var visited:Map[Long,Boolean]=Map()
    q+=neighbourVerticesMap.head._1;
    neighbourVerticesMap.foreach(k=> {
      visited += (k._1 -> false)
    })
//    q.foreach(c=>print("q"+c))
//    println("visited:"+visited)
    visited.update(q.head,true)
    var countVisitred=1;
    //println("component: ")
    while (q.size>0)
      {
        val h=q.head
//        println(h)
        q-=q.head;
       neighbourVerticesMap.get(h).foreach(u=>u.foreach(x=>{
         if(visited.get(x).head==false)
         {
           q += x
           visited.update(x,true)
           countVisitred+=1;
         }
       }))

      }
    (countVisitred==visited.size)
  }

  def getComponentByVr(graph:Graph[(String,String), String], vrid: VertexId):Graph[(String,String), String]= {

    // Filter the graph to contain only edges matching the edgeProperty
//    val filteredG = graph//.subgraph(epred = e => e.attr != edgeProperty)

    // Find the connected components of the subgraph, and cache it because we
    // use it more than once below
    val components: VertexRDD[VertexId] =
    graph.connectedComponents().vertices.cache()

    // Get the component id of the source vertex
    val sourceComponent: VertexId = components.filter {
      case (id, component) => id == vrid
    }.map(_._2).collect().head

    // Print the vertices in that component
    val cc=components.filter {
      case (id, component) => component == sourceComponent
    }.map(_._1).collect
    graph.subgraph(vpred = (id,atrr)=>cc.contains(id))
  }


  /**tested not good:
    *
    * @param graph
    * @param vrid
    * @return
    */
  def getComponent_check(graph:Graph[(String,String), String], vrid: VertexId):Graph[(String,String), String]= {
    val newGraph = Graph(
      graph.vertices.filter { case (vid, attr) => vid == vrid } ++
        graph.collectNeighbors(EdgeDirection.Either)
          .filter { case (vid, arr) => vid == vrid }
          .flatMap { case (vid, arr) => arr },
      graph.edges
    ).subgraph(vpred = {
      case (vid, attr) => attr != null
    })
    newGraph
  }

  /**tested not good: checkn agian????
    *
    * @param g
    * @param component
    * @tparam VD
    * @tparam ED
    * @return
    */
  def getSmallestComponent[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], component: VertexId): Graph[VD, ED] = {
    val cc: Graph[VertexId, ED] = ConnectedComponents.run(g)
    // Join component ID to the original graph.
    val joined = g.outerJoinVertices(cc.vertices) {
      (vid, vd, cc) => (vd, cc)
    }
    // Filter by component ID.
    val filtered = joined.subgraph(vpred = {
      (vid, vdcc) => vdcc._2 == Some(component)
    })
    // Discard component IDs.
    filtered.mapVertices {
      (vid, vdcc) => vdcc._1
    }
  }

//  def myBFs(graph: Graph[(String,String),String]):Boolean={
//
//  }
  /**
    * Returns the shortest directed-edge path from src to dst in the graph. If no path exists, returns
    * the empty list.
    */
//  def bfs[VD, ED](graph: MyGraph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
//    if (src == dst) return List(src)
//
//    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
//    var g: MyGraph[(Int, VertexId), ED] =
//      graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()
//
//    // Traverse forward from src
//    var dstAttr = (Int.MaxValue, 0L)
//    while (dstAttr._1 == Int.MaxValue) {
//      val msgs = g.aggregateMessages[(Int, VertexId)](
//        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
//          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
//        },
//        (a, b) => if (a._1 < b._1) a else b).cache()
//
//      if (msgs.count == 0) return List.empty
//
//      g = g.ops.joinVertices(msgs) {
//        (id, oldAttr, newAttr) =>
//          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
//      }.cache()
//
//      dstAttr = g.vertices.filter(_._1 == dst).first()._2
//    }
//
//    // Traverse backward from dst and collect the path
//    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
//    while (path.head != src) {
//      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
//    }
//
//    path
//  }

}
