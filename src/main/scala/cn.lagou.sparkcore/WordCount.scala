package cn.lagou.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建Spark Context
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)
    // 2.读取数据文件
    val lines: RDD[String] = sc.textFile(args(0))
    // 3.RDD转换
    val words: RDD[String] = lines.flatMap(line => line.split("//s+"));
    val wordMap: RDD[(String, Int)] = words.map(x => (x, 1))
    val result: RDD[(String, Int)] = wordMap.reduceByKey(_+_)
    // 4.输出结果
    result.foreach(println)
    // 5.关闭Spark Context
    sc.stop()
    // 6.打包程序, 使用Spark Submit提交集群运行

  }
}
