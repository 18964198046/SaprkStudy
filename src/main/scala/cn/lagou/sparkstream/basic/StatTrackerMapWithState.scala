package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StatTrackerMapWithState {

  def main(args: Array[String]): Unit = {

    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置检查点，保存状态，在生产中应该设置到HDFS
    ssc.checkpoint("data/checkpoint/")

    // 创建DStream
    val lines = ssc.socketTextStream("localhost", 9999)

    // 转换DStream
    val pairsDStream: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1))

    // (KeyType, Option[ValueType], State[StateType]) => MappedType
    def mappingFunction(key: String, one: Option[Int], state: State[Int]): (String, Int) = {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      (key, sum)
    }

    val spec = StateSpec.function(mappingFunction _)
    val resultDStream: DStream[(String, Int)] = pairsDStream.mapWithState[Int, (String, Int)](spec)
    resultDStream.cache()

    // 转换DStream
    resultDStream.print
    //resultDStream.repartition(1).saveAsTextFiles("data/output1/")

    // 输出DStream

    // 启动作业
    ssc.start()
    ssc.awaitTermination()

  }

}
