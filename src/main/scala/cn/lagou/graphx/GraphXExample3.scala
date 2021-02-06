package cn.lagou.graphx

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}


object GraphXExample3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    // s

    // 生成图
    val edgeFilePath = this.getClass.getClassLoader.getResource("data/graph.dat").toString
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFilePath)
    //graph.vertices.foreach(println)
    //graph.edges.foreach(println)

    // 调用联通图算法
    graph.connectedComponents().vertices.sortBy(_._2).foreach(println)

    sc.stop()

  }

}
