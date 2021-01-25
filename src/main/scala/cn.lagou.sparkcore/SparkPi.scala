package cn.lagou.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

object SparkPi {
  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getCanonicalName.init)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // 2.生成RDD
    val slices = if (args.length > 0) args(0).toInt else 6
    val N = 10000000;
    val n = sc.makeRDD(1 to N, slices)
      .map( idx => {
        val (x, y) = (random, random)
        if (x * x + y * y <= 1) 1 else 0
    }).sum();
    // 3.结果输出
    val pi = 4 * n / N
    println(s"pi = $pi")
    // 4.关闭SparkContext
    sc.stop();
  }
}
