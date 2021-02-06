package cn.lagou.sparkstream.kafka

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable


object OffsetsWithRedisUtils {

  // 获取Redis参数
  private val redisHost = "linux121"
  private val redisPort = 7001

  // 获取Redis的连接
  private val config = new JedisPoolConfig
  // 最大空闲数
  config.setMaxIdle(5)
  // 最大连接数
  config.setMaxTotal(10)

  private val pool = new JedisPool(config, redisHost, redisPort, 10000)
  private def getRedisConnection: Jedis = pool.getResource

  private val topicPrefix = "kafka:topic"

  private def getKey(topic: String, groupId: String) = s"$topicPrefix:$topic:$groupId"

  // 根据key获取offsets
  def getOffsetsFromRedis(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = getRedisConnection
    val offsets: Array[mutable.Map[TopicPartition, Long]] = topics.map { topic =>
      val key = getKey(topic, groupId)
      import scala.collection.JavaConverters._
      jedis.hgetAll(key)
        .asScala
        .map{ case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong }
    }
    // 归还资源
    jedis.close()
    offsets.flatten.toMap
  }

  // 将offsets保存到redis中
  def saveOffsetsToRedis(offsetRanges: Array[OffsetRange], groupId: String) = {
    // 获取连接
    val jedis: Jedis = getRedisConnection

    // 组织数据
    offsetRanges
      .map{ offsetRange =>(offsetRange.topic, (offsetRange.partition.toString, offsetRange.untilOffset.toString)) }
      .groupBy(_._1)
      .foreach{case (topic, buffer) =>
        val key = getKey(topic, groupId)

        import scala.collection.JavaConverters._
        val maps: util.Map[String, String] = buffer.map(_._2).toMap.asJava

        // 保存数据 key Map[partition, offset]
        jedis.hmset(key, maps)
      }

    jedis.close()
  }

  def main(args: Array[String]): Unit = {
    val groupId: String = "group01"
    val topics: Array[String] = Array("topicB")
    val offsets: Map[TopicPartition, Long] = getOffsetsFromRedis(topics, groupId)
    offsets.foreach(println)

  }


}

// hmset kafka:topic:topicB:group01 0 0 1 100 2 200
