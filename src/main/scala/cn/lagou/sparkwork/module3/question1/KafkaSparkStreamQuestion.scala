package cn.lagou.sparkwork.module3.question1

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.io.Source

/**
 * 最终直接结果如下:
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2993, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59162|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931058)
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2994, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59166|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931059)
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2995, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59168|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931060)
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2996, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59175|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931061)
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2997, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59178|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931062)
 * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2998, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59181|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931063)
 *
 */
object KafkaSparkStreamQuestion {

  /**
   * 1.创建原始SampleLog消息, 并存储到Kafka
   */
  def createRawSampleLog(): Unit = {
    // 定义Kafka参数
    val topic = "sampleRawLog"
    val brokers = "linux121:9092,linux122:9092,linux123:9092"
    val productProps = new Properties()
    productProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    productProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    productProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val simpleLogFilePath = this.getClass.getClassLoader.getResource("data/sample.log").toURI
    val lines = Source.fromFile(simpleLogFilePath, "utf-8").getLines();

    // 创建KafkaProducer
    var idx = 0l;
    val producer: KafkaProducer[Long, String] = new KafkaProducer[Long, String](productProps)

    lines.foreach(line => {
      // 发送消息
      val msg: ProducerRecord[Long, String] = new ProducerRecord[Long, String](topic, idx, line)
      producer.send(msg)
      idx = idx + 1
    })
    producer.close()
  }

  /**
   * 2.从Kafka_Topic:sampleRawLog中读取消息，处理后写入Kafka_Topic:sampleConvertLog
   */
  def convertRawSampleLog(): Unit = {

    // 初始化配置
    val producerTopic = "sampleConvertLog"
    val consumerTopic = "sampleRawLog"
    val consumerTopics = List(consumerTopic)
    val groupId = "simpleLogConvert"
    val brokers = "linux121:9092,linux122:9092,linux123:9092"
    val consumerProps = new mutable.HashMap[String, Object]
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建SparkStream
    val rawSampleLogStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](consumerTopics, consumerProps)
    )

    // 转换&输出DStream
    rawSampleLogStream.foreachRDD { (rdd, _) =>
      rdd.foreachPartition(partitionRecords => {
        val productProps = new Properties()
        productProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        productProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
        productProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        val producer: KafkaProducer[Long, String] = new KafkaProducer[Long, String](productProps)
        partitionRecords.map(record => record.value().split(",")
          .map(field => field.replaceAll("<<<!>>>", "")))
          .map(fieldVales => fieldVales.reduce((s1, s2) => s1 + '|' + s2))
          .foreach(line => {
            val msg: ProducerRecord[Long, String] = new ProducerRecord[Long, String](producerTopic, line)
            producer.send(msg)
          })
      })
    }

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 3.从Kafka中读取转换后的SampleLog消息, 执行结果如下:
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2993, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59162|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931058)
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2994, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59166|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931059)
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2995, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59168|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931060)
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2996, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59175|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931061)
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2997, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59178|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931062)
   * ConsumerRecord(topic = sampleConvertLog, partition = 0, offset = 2998, CreateTime = 1612574284690, serialized key size = -1, serialized value size = 128, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = 3111|238|20171112132922|58.223.1.112|202.102.92.18|59181|80|www.sumecjob.com||||||http://www.sumecjob.com/Social.aspx|2556931063)
   */
  def readConvertedSampleLog(): Unit = {
    // 初始化配置
    val consumerTopic = "sampleConvertLog"
    val consumerTopics = List(consumerTopic)
    val groupId = "simpleLogConvert"
    val brokers = "linux121:9092,linux122:9092,linux123:9092"
    val consumerProps = new mutable.HashMap[String, Object]
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建SparkStream
    val rawSampleLogStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](consumerTopics, consumerProps)
    )

    // 转换&输出DStream
    rawSampleLogStream.foreachRDD { (rdd, _) =>
      rdd.foreachPartition(partitionRecords => {
        partitionRecords.foreach(println)
      })
    }

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    // 1.创建原始SampleLog消息
    // createRawSampleLog()

    // 2.读取并转换SampleLog消息
    // convertRawSampleLog()

    // 3.读取转换后的SampleLog消息
    // readConvertedSampleLog()

  }


}
