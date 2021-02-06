package cn.lagou.graphx

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object GraphXExample2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 原始数据集
    val lst: Seq[(List[VertexId], List[(String, Double)])] = List(
      (List(11L, 21L, 31L), List("kw$北京" -> 1.0, "kw$上海" -> 1.0, "area$中关村" -> 1.0)),
      (List(21L, 32L, 41L), List("kw$上海" -> 1.0, "kw$天津" -> 1.0, "area$回龙观" -> 1.0)),
      (List(41L), List("kw$天津" -> 1.0, "area$中关村" -> 1.0)),
      (List(12L, 22L, 33L), List("kw$大数据" -> 1.0, "kw$spark" -> 1.0, "area$西二旗" -> 1.0)),
      (List(22L, 34L, 44L), List("kw$spark" -> 1.0, "area$五道口" -> 1.0)),
      (List(33L, 53L), List("kw$hive" -> 1.0, "kw$spark" -> 1.0, "area$西二旗" -> 1.0)),
    )

    val rawRDD: RDD[(List[VertexId], List[(String, Double)])] = sc.makeRDD(lst)

    // 创建点RDD
    // List(11L, 21L, 31L), A1 => 11 -> 112131, 21 -> 112131, 31 -> 112131
    val dotRDD: RDD[(VertexId, VertexId)] = rawRDD.flatMap{case (ids, _) =>
      ids.map(id => (id, ids.mkString.toLong))
    }

    // 创建边RDD[Edge(Long, Long, T2)]
    val edgeRDD: RDD[Edge[Int]] = dotRDD.map{case (id, ids) => Edge(id, ids, 0)}

    // 创建顶点RDD[(Long, T1)]
    val vertexRDD: RDD[(VertexId, String)] = dotRDD.map{case (id, _) => (id, "")}

    // 生成图
    val graph: Graph[String, Int] = Graph(vertexRDD, edgeRDD)

    // 调用强联通体算法
    val connectedRDD: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 定义中心的数据
    val centerVertexRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] =
      rawRDD.map { case (ids, info) => (ids.mkString.toLong, (ids, info)) }

    // join操作，拿到分组的标记
    val dateRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] =
      connectedRDD.join(centerVertexRDD).map { case (_, (v1, v2)) => (v1, v2) }

    // 合并信息
    val resultRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] = {
      // 数据聚合
      dateRDD.reduceByKey { case ((bufIds, bufInfo), (ids, info)) =>
        val newIds = bufIds ++ ids
        val newInfo = bufInfo ++ info
        // 用户Id去重
        (newIds.distinct, newInfo.groupBy(_._1).mapValues(lst => lst.map(_._2).sum).toList)
      }
    }

    resultRDD.foreach(println)

    sc.stop()

  }

}
