package cn.lagou.sparkstream.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDStream2 {

  def main(args: Array[String]): Unit = {

    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(2))

    // 定义Kafka相关参数
    val groupId: String = "mygroup01"
    val topics: Array[String] = Array("topicB")
    val kafkaParams: Map[String, Object] = getKafkaConsumerParameters(groupId)
    val offsets: collection.Map[TopicPartition, Long] = Map(
      new TopicPartition("topicB", 0) -> 100,
      // new TopicPartition("topicB", 1) -> 200,
      // new TopicPartition("topicB", 2) -> 300
    )

    // 创建DStream
    val dstream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    // 转换&输出DStream
    dstream.foreachRDD{(rdd, time) =>
      // 处理消息
      println(s"********** rdd.count = ${rdd.count()}; time = $time **********")
      // 显示offsets的信息
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val range: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${range.topic} ${range.partition} ${range.fromOffset} ${range.untilOffset}")
      }
    }

    // 启动作业
    ssc.start()
    ssc.awaitTermination()

  }

  def getKafkaConsumerParameters(groupId: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux121:9092,linux122:9092,linux123:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      // ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
  }

}
