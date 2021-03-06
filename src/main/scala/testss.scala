import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



object testss extends App {

  //var h:Murmur3Hash=new
//  println(MurmurHash3.stringHash("abc"))
//  println(MurmurHash3.stringHash("abc"))
//
//  println(MurmurHash3.stringHash("abc"))
val conf=new SparkConf().setAppName("graphtest1").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
val sc = SparkContext.getOrCreate(conf)
 sc.setLogLevel("ERROR")
//val sc=SparkContextUtils.getSparkContext
  val users: RDD[(Long, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),(8L, ("samih", "dr"))))

  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 2L, "parametric"), Edge(8L, 3L, "parametric"),
      Edge(2L, 5L, "overloading"), Edge(8L, 7L, "subtyping")))

  val graph = Graph(users, relationships)
//  val sourceVertexId: VertexId = 3L // vertex a in the example
 // val edgeProperty: String = "e1"

  // Filter the graph to contain only edges matching the edgeProperty
  //val filteredG = graph.subgraph(epred = e => e.attr != "collab")
//  val components: VertexRDD[VertexId] =
  //  filteredG.connectedComponents().vertices.cache()
  // Find the connected components of the subgraph, and cache it because we
  // use it more than once below
  var vset:Set[Long] = Set[Long]()
//
  graph.vertices.collect().foreach(v=> {
    vset=vset+v._1
  //  print(v._1+",")
  })
  //println("size:"+vset.size)
 // println()
  var allPotinalGraph:Set[MColorBSD]=Set()
  MyGraphNdSetUtils.power3(vset, 2).foreach(u=>{
 //   println("U:"+u)
    val sub=graph.subgraph(vpred = (id,attr)=>u.contains(id)).cache()
    if(MyGraphNdSetUtils.isConnectedGraph(sub))
      {

        val tmp:MColorBSD=new MColorBSD(sub);
        tmp.compute()
        allPotinalGraph+=tmp;
      }
  })
  println("allPotinalGraph:"+allPotinalGraph.size)
  allPotinalGraph.foreach(s=>{
    s.subg.edges.foreach(e=> println(e))
    println(s)

  })
//  //println("size:"+vset.size)
// // println()
//  var allPotinalGraph:Set[MColorBSD]=Set()
//  MySetUtils.power3(vset, 2).foreach(u=>{
// //   println("U:"+u)
//    val sub=graph.subgraph(vpred = (id,attr)=>u.contains(id)).cache()
//    if(MySetUtils.isConnectedGraph(sub))
//      {
//
//        val tmp:MColorBSD=new MColorBSD(sub);
//        tmp.compute()
//        allPotinalGraph+=tmp;
//      }
//  })
//  println("allPotinalGraph:"+allPotinalGraph.size)
//  allPotinalGraph.foreach(s=>{
//    s.subg.edges.foreach(e=> println(e))
//    println(s)
//  })
    //    println("edgs")
//    sub.edges.foreach(e=>print(e))
//    println()
//    println("verts")
//    sub.vertices.foreach(e=>print(e))
//    println()
//    val neighbourVerticesMap = sub.collectNeighborIds(EdgeDirection.Either)
//      .collect().map(vertex => (vertex._1.asInstanceOf[Long], vertex._2.toSet))
//      .toMap;
//    neighbourVerticesMap.foreach(v=>println("NeighborIds:"+v))

 //vset=Set(30)
//  val subg=graph.subgraph(vpred = (id,attr) => vset.contains(id)).cache()
//  private val neighbourVerticesMap = subg.collectNeighborIds(EdgeDirection.Either)
//    .collect().map(vertex => (vertex._1.asInstanceOf[Long], vertex._2.toSet))
//    .toMap;
//  neighbourVerticesMap.foreach(v=>println("NeighborIds:"+v))
//  println(MySetUtils.isConnectedGraph(subg))
  //******** tests: map List
//  neighbourVerticesMap.foreach(v=>println("NeighborIds:"+v))
//  var q:ListBuffer[Long]=ListBuffer();
//  var visited:Map[Long,Boolean]=Map()
//  q+=neighbourVerticesMap.head._1;
//  neighbourVerticesMap.foreach(k=> {
//    visited += (k._1 -> false)
//  })
//  neighbourVerticesMap.get(7).foreach(u=>u.foreach(x=>println(x)))
//  q+=20;
//  q+=(30,33)
//  println(" q: ")
//    q.foreach(c=>print(+c))
//
//  println(" q: ")
//  q-=q.head
//  q.foreach(c=>print(" "+c))
//
//  println()
//  println("visited:"+visited)
//
//  visited.update(q.head,true)
//  println("visited:"+visited)
//
//  val conng=MySetUtils.getBFS(vset.head,subg).cache()
//  subg.vertices.foreach(v=> println("subg:"+v._2))
//
//  conng.vertices.foreach(v=>println("conng"+v._1+ " "+ v._2))

  // Get the component id of the source vertex
//  val sourceComponent: VertexId = components.filter {
//    case (id, component) => id == sourceVertexId
//  }.map(_._2).collect().head
//
//  // Print the vertices in that component
//  components.filter {
//    case (id, component) => component == sourceComponent
//  }.map(_._1).collect.foreach(println)
  //val bk = new BronKerboschSCALA(sc, graph).runAlgorithm;

  //bk.foreach { println }

  val s:String ="SET NOCOUNT ON;" +
    "CREATE TABLE #tree(node_l CHAR(1),node_r CHAR(1));" +
    "CREATE NONCLUSTERED INDEX NIX_tree_node_l ON #tree(node_l)INCLUDE(node_r); -- covering indices to speed up lookup" +
    "CREATE NONCLUSTERED INDEX NIX_tree_node_r ON #tree(node_r)INCLUDE(node_l);" +
    "INSERT INTO #tree(node_l,node_r) VALUES('a','c'),('b','f'),('a','g'),('c','h'),('b','j'),('d','f'),('e','k'),('i','i'),('l','h'); -- test set 1" +
    "--('a','f'),('a','g'),(CHAR(0),'a'),('b','c'),('b','a'),('b','h'),('b','j'),('b',CHAR(0)),('b',CHAR(0)),('b','g'),('c','k'),('c','b'),('d','l')" +
    ",('d','f'),('d','g'),('d','m'),('d','a'),('d',CHAR(0)),('d','a'),('e','c'),('e','b'),('e',CHAR(0)); -- test set 2" +
    "--('a','a'),('b','b'),('c','a'),('c','b'),('c','c'); -- test set 3 " +
    "CREATE TABLE #sets(node CHAR(1) PRIMARY KEY,group_id INT); -- nodes with group id assigned" +
    "CREATE TABLE #visitor_queue(node CHAR(1)); -- contains nodes to visit" +
    "CREATE TABLE #visited_nodes(node CHAR(1) PRIMARY KEY CLUSTERED WITH(IGNORE_DUP_KEY=ON)); -- nodes visited for nodes on the queue; ignore duplicate nodes when inserted" +
    "CREATE TABLE #visitor_ctx(node_l CHAR(1),node_r CHAR(1)); -- context table, contains deleted nodes as they are visited from #tree" +
    "DECLARE @last_created_group_id INT=0;" +
    "-- Notes:" +
    "-- 1. This algorithm is destructive in its input set, ie #tree will be empty at the end of this procedure" +
    "-- 2. This algorithm does not accept NULL values. Populate #tree with CHAR(0) for NULL values (using ISNULL(source_col,CHAR(0)), or COALESCE(source_col,CHAR(0)))" +
    "-- 3. When selecting from #sets, to regain the original NULL values use NULLIF(node,CHAR(0))" +
    "WHILE EXISTS(SELECT*FROM #tree)" +
    "BEGIN" +
    "    TRUNCATE TABLE #visited_nodes;" +
    "    TRUNCATE TABLE #visitor_ctx;" +
    "    -- push first nodes onto the queue (via #visitor_ctx -> #visitor_queue)" +
    "    DELETE TOP (1) t" +
    "    OUTPUT deleted.node_l,deleted.node_r INTO #visitor_ctx(node_l,node_r)" +
    "    FROM #tree AS t;" +
    "    INSERT INTO #visitor_queue(node) SELECT node_l FROM #visitor_ctx UNION SELECT node_r FROM #visitor_ctx; -- UNION to filter when node_l equals node_r" +
    "    INSERT INTO #visited_nodes(node) SELECT node FROM #visitor_queue; -- keep track of nodes visited" +
    "    -- work down the queue by visiting linked nodes in #tree; nodes are deleted as they are visited" +
    "    WHILE EXISTS(SELECT*FROM #visitor_queue)" +
    "    BEGIN" +
    "        TRUNCATE TABLE #visitor_ctx;" +
    "        -- pop_front for node on the stack (via #visitor_ctx -> @node)" +
    "        DELETE TOP (1) s" +
    "        OUTPUT deleted.node INTO #visitor_ctx(node_l)" +
    "        FROM #visitor_queue AS s;" +
    "        DECLARE @node CHAR(1)=(SELECT node_l FROM #visitor_ctx); " +
    "        TRUNCATE TABLE #visitor_ctx;" +
    "        -- visit nodes in #tree where node_l or node_r equal target @node; " +
    "        -- delete visited nodes from #tree, output to #visitor_ctx" +
    "        DELETE t" +
    "        OUTPUT deleted.node_l,deleted.node_r INTO #visitor_ctx(node_l,node_r)" +
    "        FROM #tree AS t" +
    "        WHERE t.node_l=@node OR t.node_r=@node;" +
    "        -- insert visited nodes in the queue that haven't been visited before" +
    "        INSERT INTO #visitor_queue(node) " +
    "        (SELECT node_l FROM #visitor_ctx UNION SELECT node_r FROM #visitor_ctx) EXCEPT (SELECT node FROM #visited_nodes);" +
    "" +
    "        -- keep track of visited nodes (duplicates are ignored by the IGNORE_DUP_KEY option for the PK)" +
    "        INSERT INTO #visited_nodes(node)" +
    "        SELECT node_l FROM #visitor_ctx UNION SELECT node_r FROM #visitor_ctx;" +
    "    END" +
    "    SET @last_created_group_id+=1; -- create new group id" +
    "    -- insert group into #sets" +
    "    INSERT INTO #sets(group_id,node)" +
    "    SELECT group_id=@last_created_group_id,node " +
    "    FROM #visited_nodes;" +
    "END" +
    "" +
    "SELECT node=NULLIF(node,CHAR(0)),group_id FROM #sets ORDER BY node; -- nodes with their assigned group id" +
    "" +
    "SELECT g.group_id,m.members  -- groups with their members" +
    "FROM" +
    "   (SELECT DISTINCT group_id FROM #sets) AS g" +
    "    CROSS APPLY (" +
    "        SELECT members=STUFF((" +
    "                SELECT ','+ISNULL(CAST(NULLIF(si.node,CHAR(0)) AS VARCHAR(4)),'NULL')" +
    "                FROM #sets AS si " +
    "                WHERE si.group_id=g.group_id" +
    "                FOR XML PATH('')" +
    "            ),1,1,'')" +
    "     ) AS m" +
    "ORDER BY g.group_id;" +
    "DROP TABLE #visitor_queue;" +
    "DROP TABLE #visited_nodes;" +
    "DROP TABLE #visitor_ctx;" +
    "DROP TABLE #sets;" +
    "DROP TABLE #tree;"



}
