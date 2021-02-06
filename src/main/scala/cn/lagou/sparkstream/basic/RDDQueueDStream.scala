package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object RDDQueueDStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(conf, Seconds(1))

    // 创建DStream
    val queue = Queue[RDD[Int]]()
    val queueDStream: InputDStream[Int] = ssc.queueStream(queue)

    // 转换DStream
    val result: DStream[(Int, Int)] = queueDStream.map(elem => (elem % 10, 1)).reduceByKey(_ + _)

    // 输出DStream
    result.print

    // 启动作业
    ssc.start()

    // 每秒生成一个RDD, 将RDD放置在队列中
    for (i <- 1 to 5) {
      queue.synchronized {
        val range = (1 to 100).map(_ * i)
        queue += ssc.sparkContext.makeRDD(range, 2)
      }
      Thread.sleep(1000)
    }

    ssc.stop()
  }

}
