package cn.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SuperWordCount {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkContext
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(this.getClass.getCanonicalName.init)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 2.生成RDD
    val stopWords = "in on to a an the are were si was some any".split("\\s+")
    val punctuation = "[\\(\\),.:;'\\?!]";
    val lines: RDD[String] = sc.textFile("/data/swc.txt")
    lines.flatMap(line => line.split("\\s+"))
      .map(_.toLowerCase.replaceAll(punctuation, ""))
      .filter(!stopWords.contains(_))
      .map((_, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false).collect().foreach(println)

    //.collect().foreach(println)


    sc.stop()
  }




}
