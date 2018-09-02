// Java program to print BFS traversal from a given source vertex.
// BFS(int s) traverses vertices reachable from s.
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import scala.collection.Map;
import scala.collection.immutable.Set;
import scala.collection.mutable.HashMap;

import java.io.*;
import java.util.*;

// This class represents a directed graph using adjacency list
// representation
class MyGraph<V_1,V_2,Et>{
    private int V;   // No. of vertices
    private LinkedList<Integer> adj[]; //Adjacency Lists
    private HashMap<Long, Set<Long>> hashMap=new HashMap<>();
    private Map<Long, Set<Long>> mapAdj=new HashMap<>();


        // Constructor
    MyGraph(int v) {
        V = v;
        adj = new LinkedList[v];
        for (int i = 0; i < v; ++i)
            adj[i] = new LinkedList();
    }

    // Function to add an edge into the graph
    void addEdge(long v, long w) {
        adj[(int) v].add((int) w);
    }

    void  addEdgs(Graph<String,String> graph)
    {
        //graph.ops().collectNeighborIds()
//        for (Edge e:graph.edges().cache().collect()) {
//            addEdge(e.srcId(),e.dstId());
//        }
    }

    // prints BFS traversal from a given source s
    void BFS(int s) {
        // Mark all the vertices as not visited(By default
        // set as false)
        boolean visited[] = new boolean[V];

        // Create a queue for BFS
        LinkedList<Integer> queue = new LinkedList<Integer>();

        // Mark the current node as visited and enqueue it
        visited[s] = true;
        queue.add(s);

        while (queue.size() != 0) {
            // Dequeue a vertex from queue and print it
            s = queue.poll();
            System.out.print(s + " ");

            // Get all adjacent vertices of the dequeued vertex s
            // If a adjacent has not been visited, then mark it
            // visited and enqueue it
            Iterator<Integer> i = adj[s].listIterator();
            while (i.hasNext()) {
                int n = i.next();
                if (!visited[n]) {
                    visited[n] = true;
                    queue.add(n);
                }
            }
        }
    }
}