package cn.lagou.sparkwork.question3

import java.util.UUID

import org.apache.spark.sql.{Encoders, SparkSession}



/**
 * +----+---+-----+
 * |adId|imp|click|
 * +----+---+-----+
 * |  31|  2|    2|
 * |  32|  1|    0|
 * |  33|  1|    0|
 * |  34|  0|    1|
 * +----+---+-----+
 */
object ClickAndImpAnalyze {

  case class RequestLogRecord(logLevel: String, day: String, time: String, uri: String) {
    var id: String = UUID.randomUUID().toString
    var resource: String = uri.split("/")(1).split("\\?")(0)
    var adId: Int = uri.split("\\?")(1).split("&").filter(s => s.contains("adid"))(1).split("=")(1).toInt
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val impFilePath = this.getClass.getClassLoader.getResource("data/imp.log").toString
    val clickFilePath = this.getClass.getClassLoader.getResource("data/click.log").toString

    import spark.implicits._
    val imp = spark.read
      .option("delimiter", " ")
      .schema(Encoders.product[RequestLogRecord].schema)
      .csv(impFilePath)
      .as[RequestLogRecord]

    val click = spark.read
      .option("delimiter", " ")
      .schema(Encoders.product[RequestLogRecord].schema)
      .csv(clickFilePath)
      .as[RequestLogRecord]

    imp.show(10, false)
    click.show(10, false)

    val impRdd = imp.rdd.map(record => {
      val impCount = if (record.resource.equals("imp")) 0 else 1
      val clickCount = if (record.resource.equals("click")) 0 else 1
      (record.adId, (impCount, clickCount))
    })

    val clickRdd = click.rdd.map(record => {
      val impCount = if (record.resource.equals("imp")) 0 else 1
      val clickCount = if (record.resource.equals("click")) 0 else 1
      (record.adId, (impCount, clickCount))
    })

    val unionRdd = impRdd.union(clickRdd)
      .aggregateByKey((0,0))((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2), (s3, s4) => (s3._1 + s4._1, s3._2 + s4._2))
      .sortByKey()
      .map(r => (r._1, r._2._1, r._2._2))

    unionRdd.toDF("adId", "imp", "click").show()

  }

}
