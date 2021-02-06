package cn.lagou.question

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object AirportGraphxQuestion {

  /**
   * *******打印所有的顶点*******
   * (3,DFW)
   * (1,SFO)
   * (2,ORD)
   *
   * *******打印所有的边*******
   * Edge(3,1,1400)
   * Edge(1,2,1800)
   * Edge(2,3,800)
   *
   * *******打印所有的triplet*******
   * ((1,SFO),(2,ORD),1800)
   * ((2,ORD),(3,DFW),800)
   * ((3,DFW),(1,SFO),1400)
   *
   * *******打印顶点数*******
   * 3
   *
   * *******打印边*******
   * 3
   *
   * *******打印机场距离大于1000，并降序输出*******
   * Edge(1,2,1800)
   * Edge(3,1,1400)
   *
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 原始数据集
    val lst: Seq[(VertexId, String, List[(VertexId, VertexId, Int)])] = List(
      (1, "SFO", List((1, 2, 1800))),
      (2, "ORD", List((2, 3, 800))),
      (3, "DFW", List((3, 1, 1400)))
    )

    val rawRDD: RDD[(VertexId, String, List[(VertexId, VertexId, Int)])] = sc.makeRDD(lst)

    // 创建边
    val edgeRDD: RDD[Edge[Int]] = rawRDD.flatMap{ case (_, _, info) =>
      info.map { case (srcId, dstId, attr) => Edge(srcId, dstId, attr)}
    }

    // 创建顶点
    val vertexRDD: RDD[(VertexId, String)] = rawRDD.map{case (id, name, _) => (id, name)}

    // 创建图
    val graph = Graph(vertexRDD, edgeRDD);

    // (1)求所有的顶点
    println("*******打印所有的顶点*******")
    graph.vertices.foreach(println)

    // (2)求所有的边
    println("*******打印所有的边*******")
    graph.edges.foreach(println)

    // (3)求所有的triplets
    println("*******打印所有的triplet*******")
    graph.triplets.foreach(println)

    // (4)求顶点数
    println("*******打印顶点数*******")
    println(graph.vertices.count())

    // (5)求边数
    println("*******打印边*******")
    println(graph.edges.count())

    // (6)求机场距离大于1000有几个，按将需输出结果
    println("*******打印机场距离大于1000，并降序输出*******")
    graph.edges.filter(edge => edge.attr > 1000).sortBy(_.attr, true).foreach(println)

  }

}
