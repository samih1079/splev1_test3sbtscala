import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import MyGraphNdSetUtils._
import SimTypeMames._
import scala.reflect.ClassTag

object TestComponent extends App {
println("kkk")
  val conf=new SparkConf().setAppName("graphtest1").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("ERROR")
  //val sc=SparkContextUtils.getSparkContext
  val users: RDD[(Long, (String, String))] =
    sc.parallelize(Array((3L, ("p1", "c11")), (7L, ("p1", "c12")),
      (5L, ("p2", "c21")), (2L, ("p2", "c22")),(8L, ("p3", "c31")),(9L, ("p1", "c13"))))

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 2L, "parametric"), Edge(3L, 5L, "parametric"),
      Edge(9L, 5L, "overloading"), Edge(8L, 7L, non),Edge(2L, 9L, subt)))

  val graph = Graph(users, relationships)
  //val cc=graph.connectedComponents().edges.foreach(println)
//given a VertexId in a source graph, create a new graph with the nodes and edges connected to this VertexId from
// the source graph.
  //....
  //val vrid=8
  //SimilarityDegree.getMColorForPRog(graph,"p1",2)

  SimilarityDegree.getMcolorPrVDSetByForProgram(graph.subgraph(epred = e=> e.attr!=SimTypeMames.non),"p1",2);
//  println("connectedComponents():")
//  getComponentByVr(graph.subgraph(epred = e=> e.attr=="parametric"),2).edges.foreach(println)


  //  val sourceVertexId: VertexId = 3L // vertex a in the example
//  val edgeProperty: String = "e1"
//
//  // Filter the graph to contain only edges matching the edgeProperty
//  val filteredG = graph//.subgraph(epred = e => e.attr != edgeProperty)
//
//  // Find the connected components of the subgraph, and cache it because we
//  // use it more than once below
//  val components: VertexRDD[VertexId] =
//  filteredG.connectedComponents().vertices.cache()
//
//  // Get the component id of the source vertex
//  val sourceComponent: VertexId = components.filter {
//    case (id, component) => id == sourceVertexId
//  }.map(_._2).collect().head
//
//  // Print the vertices in that component
//  components.filter {
//    case (id, component) => component == sourceComponent
//  }.map(_._1).collect.foreach(println)



}
