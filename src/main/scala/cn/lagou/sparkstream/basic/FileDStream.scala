package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileDStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建DStream
    val lines: DStream[String] = ssc.textFileStream("/wcinput/")

    // 转换DStream
    val words: DStream[String] = lines.flatMap(line => line.split("\\s+"))
    val result: DStream[(String, Int)] = words.map(word => (word, 1)).reduceByKey(_ + _)

    // 输出DStream
    result.print

    // 启动作业
    ssc.start()
    ssc.awaitTermination()

  }

}
