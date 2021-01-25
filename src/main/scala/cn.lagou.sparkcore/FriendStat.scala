package cn.lagou.sparkcore

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Buffer

object FriendStat {

  /**
   * 原始数据:
   * 100, 200 300 400 500 600
   * 200, 100 300 400
   * 300, 100 200 400 500
   * 400, 100 200 300
   * 500, 100 300
   * 600, 100
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getCanonicalName.init)
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("/data/friends.txt")

//    方法一: 笛卡尔积
//    val friendsRDD: RDD[(String, Buffer[String])] =
//      lines.map(line => line.trim.split(","))
//        .map(fields => (fields(0), fields(1).trim.split("\\s+").toBuffer))
//
//    friendsRDD.cartesian(friendsRDD)
//      .filter{case ((id1, _), (id2, _)) => id1 < id2}
//      .map{case ((id1, friends1), (id2, friends2)) => ((id1, id2), friends1.intersect(friends2).sorted)}
//      .filter{case (_, friends) => friends.size > 0}
//      .sortByKey()
//      .collect.foreach(println)

    // 方法二:
//    val friendsRDD: RDD[((String, String), Buffer[String])] =
//      lines.map(line => line.trim.split(","))
//        .map(fields => (fields(0), fields(1).trim.split("\\s+").toBuffer))
//        .flatMapValues(friends => (for (friend1 <- friends.iterator; friend2 <- friends.iterator) yield(friend1, friend2)).filter{case (id1, id2) => id1 != id2})
//        .mapValues {case (id1, id2) => if (id1 < id2) (id1, id2) else (id2, id1)}
//        .map {case (id1, (id2, id3)) => ((id2, id3), id1)}
//        .reduceByKey(_+ " " + _)
//        .mapValues(friends => friends.split("\\s+").distinct.toBuffer)
//        .sortByKey()//.collect().foreach(println)

    lines.map(line => line.trim.split(","))
      .map(fields => (fields(0), fields(1).trim.split("\\s+")))
      .flatMapValues(friends => friends.combinations(2))
      .map{case (id, friends) => (friends.toBuffer, id)}
      .map{case (pair, friends) => ((pair(0), pair(1)), friends)}
      .reduceByKey(_ + " " + _)
      .sortByKey()
      //.mapValues(friends => friends.trim.split("\\s+").distinct.toBuffer)
      .collect().foreach(println)

    sc.close
  }

}
