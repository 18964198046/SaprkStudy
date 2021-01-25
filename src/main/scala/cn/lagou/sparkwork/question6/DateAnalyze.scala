package cn.lagou.sparkwork.question6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lead
import org.apache.spark.sql.expressions.Window

object DateAnalyze {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    val dateFilePath = this.getClass.getClassLoader.getResource("data/date.csv").toString

    val df1 = spark.read
      .option("header", "true")
      .option("delimiter", " ")
      .csv(dateFilePath)

    df1.createTempView("t1")

    /**
     * 1.合并日期并排序
     * +----------+
     * | startdate|
     * +----------+
     * |2019-03-04|
     * |2019-10-09|
     * |2020-02-03|
     * |2020-04-05|
     * |2020-06-11|
     * |2020-08-04|
     * +----------+
     */
    val df2 = spark.sql(
      """
        |select startdate
        |  from (select startdate from t1
        |          union
        |        select enddate from t1)
        | order by startdate
        |""".stripMargin)

    /**
     * 2.使用lead函数处理数据
     * +----------+----------+
     * | startdate|   enddate|
     * +----------+----------+
     * |2019-03-04|2019-10-09|
     * |2019-10-09|2020-02-03|
     * |2020-02-03|2020-04-05|
     * |2020-04-05|2020-06-11|
     * |2020-06-11|2020-08-04|
     * |2020-08-04|      null|
     * +----------+----------+
     */
    val column = Window.orderBy("startdate")
    df2.withColumn("enddate",
      lead("startdate", 1, null).over(column)).createTempView("t2")

    /**
     * 3.处理null值
     * +----------+----------+
     * | startdate|   enddate|
     * +----------+----------+
     * |2019-03-04|2019-10-09|
     * |2019-10-09|2020-02-03|
     * |2020-02-03|2020-04-05|
     * |2020-04-05|2020-06-11|
     * |2020-06-11|2020-08-04|
     * |2020-08-04|2020-08-04|
     * +----------+----------+
     */
    spark.sql(
      """
        |select startdate, nvl(enddate, startdate) enddate
        |  from t2
        |""".stripMargin).show

  }


}
