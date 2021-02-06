package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HotWordStats {

  def main(args: Array[String]): Unit = {

    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置检查点，保存状态，在生产中应该设置到HDFS
    ssc.checkpoint("data/checkpoint")

    // 创建DStream
    val lines = ssc.socketTextStream("localhost", 9999)

    // 转换DStream
    val wordStat1 = lines.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(20), Seconds(10))
    wordStat1.print

    // window2 = w1 - t1 - t2 + t4 + t5
    // 需要checkpoint的支持
    val wordStat2 = lines.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(10))
    wordStat2.print

    // 启动作业
    ssc.start()
    ssc.awaitTermination()

  }

}
