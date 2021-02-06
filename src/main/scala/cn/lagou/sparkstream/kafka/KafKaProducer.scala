package cn.lagou.sparkstream.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafKaProducer {

  def main(args: Array[String]): Unit = {

    // 定义Kafka参数
    val brokers = "linux121:9092,linux122:9092,linux123:9092"
    val topic = "topicB"
    val prop = new Properties()

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    // KafkaProducer
    val producer = new KafkaProducer[String, String](prop)
    for (i <- 1 to 1000000) {
      // 发送消息
      val msg = new ProducerRecord[String, String](topic, i.toString, i.toString)
      producer.send(msg)
      println(s"i = $i")
      Thread.sleep(100)
    }
    producer.close()
  }

}
