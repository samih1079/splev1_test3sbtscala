import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object TestComponent extends App {
println("kkk")
  val conf=new SparkConf().setAppName("graphtest1").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("ERROR")
  //val sc=SparkContextUtils.getSparkContext
  val users: RDD[(Long, (String, String))] =
    sc.parallelize(Array((3L, ("p1", "c11")), (7L, ("p1", "c12")),
      (5L, ("p2", "c21")), (2L, ("p2", "c22")),(8L, ("p3", "c31"))))

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 2L, "parametric"), Edge(5L, 3L, "parametric"),
      Edge(2L, 5L, "overloading"), Edge(8L, 7L, "subtyping")))

  val graph = Graph(users, relationships)
  //val cc=graph.connectedComponents().edges.foreach(println)
//given a VertexId in a source graph, create a new graph with the nodes and edges connected to this VertexId from
// the source graph.
  val vrid=7
  val newGraph = Graph(
    graph.vertices.filter{case (vid,attr) => vid == vrid} ++
      graph.collectNeighbors(EdgeDirection.Either)
        .filter{ case (vid,arr) => vid == vrid}
        .flatMap{ case (vid,arr) => arr},
    graph.edges
  ).subgraph(vpred = { case (vid,attr) => attr != null}).edges.foreach(println)


  def getComponent[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED], component: VertexId): Graph[VD, ED] = {
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

}
