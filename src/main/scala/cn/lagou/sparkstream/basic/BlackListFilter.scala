package cn.lagou.sparkstream.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackListFilter {

  def main(args: Array[String]): Unit = {

    // 初始化
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(5))

    // 自定义黑名单数据
    val blackList: Array[(String, Boolean)] = Array(("Spark", true), ("Scala", true))
    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.makeRDD(blackList)

    // 创建DStream, 使用ConstantInputDStream
    val words = "Hadoop Spark Kafka Hive Zookeeper Scala Hbase Flume Sqoop"
      .split("\\s+")
      .zipWithIndex
      .map { case (word, timestamp) => s"$timestamp $word" }

    val rdd: RDD[String] = ssc.sparkContext.makeRDD(words)
    val wordDStream: ConstantInputDStream[String] = new ConstantInputDStream(ssc, rdd)

    // 流式数据的处理, 使用transform
    //    wordDStream.transform { rdd =>
    //      rdd.map{ line => (line.split("\\s+")(1), line) }
    //        .leftOuterJoin(blackListRDD)
    //        .filter{ case(_, (_, value)) => value == None}
    //        .map{ case (_, (value, _)) => value}
    //    }

    // 流式数据的处理, 使用SQL/DSL
    wordDStream.map(line => (line.trim.split("\\s+")(1), line))
      .transform { rdd =>
        val spark = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()
        import spark.implicits._
        val wordDF: DataFrame = rdd.toDF("word", "line")
        val blackListDF: DataFrame = blackListRDD.toDF("word", "flag")
        wordDF.join(blackListDF, Seq("word"), "left_outer")
          .filter("flag is null or flag = false")
          .rdd
      }.print(20)


    // 流式数据的输出

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }

}
