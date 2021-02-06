package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo {

  def main(args: Array[String]): Unit = {

    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //    // 输出DStream
    //    lines.foreachRDD {(rdd, time) =>
    //      // 打印RDD的相关信息
    //      println(s"rdd = ${rdd.id}; time = $time")
    //      // 打印RDD中的每一个元素
    //      rdd.foreach(println)
    //    }

    // 窗口操作
    val res1 = lines.reduceByWindow(_ + " " + _, Seconds(20), Seconds(10))
    res1.print

    val res2 = lines.window(Seconds(20), Seconds(10))
    res2.print

    val res3 = res2.map(_.toInt).reduce(_ + _)
    res3.print

    val res4 = lines.map(_.toInt).reduceByWindow(_ + _, Seconds(20), Seconds(10))
    res4.print


    // 启动作业
    ssc.start()
    ssc.awaitTermination()

  }

}
