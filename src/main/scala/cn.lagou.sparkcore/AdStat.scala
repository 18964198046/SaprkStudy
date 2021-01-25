package cn.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object AdStat {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkContext
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getCanonicalName.init)
    val sc = new SparkContext(conf)

    // 2.生成RDD
    val lines: RDD[String] = sc.textFile("/data/advert.log")

    // 3.RDD转换
    // 时间 省份 城市 用户 广告
    // (1) 统计每一个省份点击Top3的广告ID
    val stat1: RDD[(String, String)] = lines.map { line =>
      val fields: Array[String] = line.split("\\s+")
      (fields(1), fields(4))
    }
    stat1.map{case (province, advertId) => ((province, advertId), 1)}
      .reduceByKey(_+_)
      .map{case ((province, advertId), count) => (province, (advertId, count))}
      .groupByKey().mapValues(buffer => buffer.toList.sortWith(_._2 > _._2)
      .take(3).map(_._1).mkString(":"))

    // (2) 统计每一个省份每一个小时的Top3广告ID
    val stat2: RDD[(Int, String, String)] = lines.map { line =>
      val fields: Array[String] = line.split("\\s+")
      ((new DateTime(fields(0).toLong).getHourOfDay(), fields(1), fields(4)), 1)
    }.reduceByKey(_+_).map{case ((hour, province, advertId),count) =>
      ((hour, province), (advertId, count))
    }.groupByKey().mapValues(buffer => buffer.toList.sortWith(_._2 > _._2).take(3).map(_._1).mkString(":"))
      .map{case ((hour, province), advertId) => (hour, province, advertId)}

      stat2.collect().foreach(println)

    // 4.结果输出

    // 5.关闭SparkContext
    sc.stop()

  }
}
