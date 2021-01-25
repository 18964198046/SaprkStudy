package cn.lagou.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSqlRowToColumn {

  case class Info(id: String, tags: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    import spark.implicits._
    val arr = Array("1 1,2,3", "2 2,3", "3 1,2")
    val rdd: RDD[Info] = sc.makeRDD(arr)
      .map(line => line.trim.split("\\s+"))
      .map(fields => Info(fields(0), fields(1)))

    val ds: Dataset[Info] = spark.createDataset(rdd)
    ds.createOrReplaceTempView("t1")

    // HQL语法
    spark.sql(
      """
        | select id, tag
        |   from t1
        |        lateral view explode(split(tags, ",")) t2 as tag
        |""".stripMargin).show()

    // SparkSQL
    spark.sql(
      """
        | select id, explode(split(tags, ",")) tag
        |   from t1
        |""".stripMargin).show()

    spark.close()

  }

}
