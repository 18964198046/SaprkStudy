package cn.lagou.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class User(name: String, age: Int, inDegrees: Int, outDegrees: Int)

object GraphXExample1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 图的定义
    // (1)定义顶点
    val vertexArray: Array[(VertexId, (String, Int))] = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    // (2)定义边
    val edgeArray: Array[Edge[Int]] = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    // 构造VertexRDD和EdgeRDD
    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.makeRDD(vertexArray);
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(edgeArray)

    // 构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD);

    // 属性操作(找出图中年龄>30的顶点; 属性 > 5的边; 属性 > 5的triplets)
    graph.vertices
      .filter{case (_,(_, age)) => age > 30}
      .foreach(println)

    graph.edges
      .filter(edge => edge.attr > 5)
      .foreach(println)

    graph.triplets
      .filter{t => t.attr > 5}
      .foreach(println)

    // 属性操作, degrees操作，找出图中最大的出度、入度、度数
    val maxInDegrees: (VertexId, Int) = graph.inDegrees.reduce((x, y) => if (x._2 > y._2) x else y)
    println(s"maxInDegrees = $maxInDegrees")

    val maxOutDegrees: (VertexId, Int) = graph.outDegrees.reduce((x, y) => if (x._2 > y._2) x else y)
    println(s"maxOutDegrees = $maxOutDegrees")

    val maxDegrees = graph.degrees.reduce((x, y) => if (x._2 > y._2) x else y)
    println(s"maxDegrees = $maxDegrees")

    // 转换操作，顶点转换，所有的年龄+1
    graph.mapVertices{case (id, (name, age)) => (id, (name, age + 100))}
      .vertices.foreach(println)

    // 边的转换，边的属性*2
    graph.mapEdges(e => e.attr * 2).edges.foreach(println)

    // 结构操作，顶点年龄>30的子图
    val subgraph: Graph[(String, Int), Int] = graph.subgraph(vpred = (_, vd) => vd._2 > 30)
    println("****************")
    subgraph.edges.foreach(println)
    println("****************")
    subgraph.vertices.foreach(println)
    println("****************")


    // 找出出度 = 入度的人员，连接操作
    // 思路: 图 + 顶点的出度 + 顶点的入度
    println("找出出度 = 入度的人员，连接操作")
    val initialUserGraph: Graph[User, Int] = graph.mapVertices{case (_, (name, age)) => User(name, age, 0, 0)}
    val userGraph: Graph[User, Int] = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (_, u, inDegrees) => User(u.name, u.age, inDegrees.getOrElse(0), u.outDegrees)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (_, u, outDegrees) => User(u.name, u.age, u.inDegrees, outDegrees.getOrElse(0))
    }
    userGraph.vertices.filter{case (_, user) => user.inDegrees == user.outDegrees}.foreach(println)
    println("****************")

    // 找出顶点5到其他各顶点的最短距离，集合操作(Pregel API)
    println("找出顶点5到其他各顶点的最短距离，集合操作(Pregel API)")
    val sourceId: VertexId = 5l
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val disGraph = initialGraph.pregel(Double.PositiveInfinity)(
      //两个消息来的时候，取其中的最小路径
      (_, dist, newDist) => math.min(dist, newDist),
      // SendMessage函数
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else Iterator.empty
      },
      // mergeMsg
      (distA, distB) => math.min(distA, distB)
    )
    disGraph.vertices.foreach(println)
    println("****************")

    sc.stop()

  }

}
