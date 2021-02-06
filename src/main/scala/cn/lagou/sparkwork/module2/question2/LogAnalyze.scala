package cn.lagou.sparkwork.module2.question2

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.SparkSession

object LogAnalyze {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val logsFilePath = "file:///" + this.getClass.getClassLoader.getResource("data/cdn.txt").getPath

    val logs = spark.read.option("delimiter", " ").csv(logsFilePath)
    logs.show(10, false)

    logs.createTempView("logs")

    /**
     * question2.1: 计算独立IP数
     * +-----+
     * |count|
     * +-----+
     * |43649|
     * +-----+
     */
    spark.sql(
      """
        |select count(1) count
        |  from (select distinct(_c0) ip from logs)
        |""".stripMargin).show

    /**
     * question2.2: 统计每个视频独立IP数
     * +------------------------------------------------+-----+
     * |video                                           |count|
     * +------------------------------------------------+-----+
     * |GET http://cdn.v.abc.com.cn/131425.mp4 HTTP/1.1 |1    |
     * |GET https://v-cdn.abc.com.cn/140400.mp4 HTTP/1.1|8    |
     * |GET http://v-cdn.abc.com.cn/140943.mp4 HTTP/1.1 |26   |
     * |GET http://v-cdn.abc.com.cn/140888.mp4 HTTP/1.1 |6    |
     * |GET http://v-cdn.abc.com.cn/140832.mp4 HTTP/1.1 |3    |
     * |HEAD http://v-cdn.abc.com.cn/140938.mp4 HTTP/1.1|1    |
     * |GET http://v-cdn.abc.com.cn/140182.mp4 HTTP/1.1 |10   |
     * |GET http://v-cdn.abc.com.cn/140401.mp4 HTTP/1.1 |2    |
     * |GET https://v-cdn.abc.com.cn/139844.mp4 HTTP/2.0|3    |
     * |GET https://v-cdn.abc.com.cn/140658.mp4 HTTP/1.1|4    |
     * +------------------------------------------------+-----+
     */
    spark.sql(
      """
        |select video, count(distinct(ip)) count
        |  from (select _c0 ip, _c5 video
        |          from logs
        |         where _c5 like '%.mp4 %')
        | group by video
        |""".stripMargin).show(10, false)


    /**
     * question2.3: 统计一天中每个小时的流量
     * +-------------+-----+
     * |         time|count|
     * +-------------+-----+
     * |2017-02-15 13|18102|
     * |2019-03-15 00|    1|
     * |2016-12-15 00|    1|
     * |2017-02-17 02|    2|
     * |2017-02-20 00|    3|
     * |2018-08-01 00|    1|
     * |2018-03-06 16|    1|
     * |2017-02-16 00|    5|
     * |2017-08-15 00|    1|
     * |2017-02-15 01| 2165|
     * |2016-12-05 20|    1|
     * |2017-02-02 01|    2|
     * |2017-02-19 02|    1|
     * |2017-02-21 12|    1|
     * |2017-02-01 15|    1|
     * |2021-02-15 05|    1|
     * |2017-02-16 03|    1|
     * |2017-02-02 03|    1|
     * |2017-02-15 03| 2068|
     * |2012-02-15 05|    1|
     * +-------------+-----+
     */
    def timeToHour(time: String): String = {
      val date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US).parse(time.replace("[", ""))
      val result = new SimpleDateFormat("yyyy-MM-dd HH").format(date)
      val year = result.split("-")(0).toInt
      if ( year >= 2000 && year <= 2022) result else null
    }

    spark.udf.register("timeToHour", timeToHour _)

    spark.sql(
      """
        |select time, count(*) count
        |  from (select timeToHour(_c3) time from logs)
        | where time is not null
        | group by time
        |""".stripMargin).show

  }

}
