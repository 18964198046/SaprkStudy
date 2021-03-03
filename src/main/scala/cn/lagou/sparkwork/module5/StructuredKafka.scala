package cn.lagou.sparkwork.module5

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class BusInfo(deployNum: String,
                   simNum: String,
                   transpotNum: String,
                   plateNum: String,
                   lglat: String,
                   speed: String,
                   direction: String,
                   mileage: String,
                   timeStr: String,
                   oilRemain: String,
                   weight: String,
                   acc: String,
                   locate: String,
                   oilWay: String,
                   electric: String)

object BusInfo {
  def apply(msg: String): BusInfo = {
    val arr: Array[String] = msg.split(",")
    new BusInfo(
      arr(0),
      arr(1),
      arr(2),
      arr(3),
      arr(4),
      arr(5),
      arr(6),
      arr(7),
      arr(8),
      arr(9),
      arr(10),
      arr(11),
      arr(12),
      arr(13),
      arr(14)
    )
  }
}

object StructuredKafka {

  def main(args: Array[String]): Unit = {

    //1 获取sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]") .appName("kafka-redis") .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    //2 定义读取kafka数据源
    import spark.implicits._
    val kafkaDf: DataFrame = spark.readStream
      .format("kafka") .option("kafka.bootstrap.servers", "hadoop4:9092")
      .option("subscribe", "lagou_bus_info")
      .load()

    //3 处理数据
    val kafkaValDf: DataFrame = kafkaDf.selectExpr("CAST(value AS STRING)") //转为ds
    val kafkaDs: Dataset[String] = kafkaValDf.as[String]
    val busInfoDs: Dataset[BusInfo] = kafkaDs.map(msg => {
      BusInfo(msg)
    })

    //4 输出,写出数据到mysql
    val writer = new MySqlWriter
    busInfoDs.writeStream.foreach(writer)
      .outputMode("append")
      .start().awaitTermination()

  }

}
