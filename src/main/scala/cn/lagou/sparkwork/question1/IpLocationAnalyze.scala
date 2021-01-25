package cn.lagou.sparkwork.question1

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession


/**
 * 执行结果:
 * +------+-----+
 * |  city|count|
 * +------+-----+
 * |  西安| 1824|
 * |  昆明|  126|
 * |石家庄|  383|
 * |  重庆|  554|
 * |  北京| 1535|
 * +------+-----+
 */
object IpLocationAnalyze {

  case class HttpRecord(timestamp:String, ip:String, domain:String, url:String, agent:String, file:String)
  case class IpRecord(ip1:String, ip2:String, range1: Long, range2: Long,
                      continent:String, country_cn:String, province:String,
                      city:String, region:String, unit:String, country_en:String,
                      country_code: String, latitude:String, longitude:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val ipsFilePath = "file:///" + this.getClass.getClassLoader.getResource("data/ip.dat").getPath
    val httpsFilePath = "file:///" + this.getClass.getClassLoader.getResource("data/http.log").getPath;

    val ipRecordSchema = Encoders.product[IpRecord].schema
    val httpRecordSchema = Encoders.product[HttpRecord].schema

    val ips = spark.read.option("delimiter", "|").schema(ipRecordSchema).csv(ipsFilePath)
    val https = spark.read.option("delimiter", "|").schema(httpRecordSchema).csv(httpsFilePath)

    ips.createTempView("ips")
    https.createTempView("https")

    // 定义ip地址到range值的转换函数: ip_1 * 256 * 256 * 256  + ip_2 * 256 * 256 + ip_3 * 256 + ip_4
    def ipToRange(ip: String): Long = {
      val parts = ip.split("\\.").map(s => s.toInt);
      parts(0) * 16777216 + parts(1) * 65536 + parts(2) * 256 + parts(3)
    }

    spark.udf.register("ipToRange", ipToRange _)

    spark.sql(
      """
        |select city, count(1) as count
        |  from ips i,
        |       (select *, ipToRange(ip) range from https) h
        |  where h.range >= i.range1 and h.range <= i.range2 and city is not null
        |  group by city
        |""".stripMargin).show

  }

}
