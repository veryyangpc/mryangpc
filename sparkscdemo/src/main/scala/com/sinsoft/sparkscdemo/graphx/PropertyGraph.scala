package com.sinsoft.sparkscdemo.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._


/**
 * 图Graphx算法demo程序
 */
object PropertyGraph extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val sc = new SparkContext(conf)
  
  // 有向图，顶点Property类型为二元数组，(String,String), 每一个顶点都有唯一的VertexId
  val users: RDD[(VertexId,(String,String))] = 
     sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // 有向图边的Property类型为String 
  val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
                       
  val defaultUser = ("John Doe", "Missing")

  val graph = Graph(users, relationships, defaultUser)
  println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count())
  println(graph.edges.filter(e => e.srcId > e.dstId).count())
}