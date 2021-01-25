package cn.lagou.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlTransformation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSqlTransformation")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    import spark.implicits._
    val df: DataFrame = spark.read.
      option("header", true).
      option("delimiter", ",").
      option("inferschema", "true").
      csv("/data/emp.dat")

    import org.apache.spark.sql.functions._
    df.groupBy("Job")
      .agg(min("sal").as("minsal"),
        max("sal").as("maxsal"))
      .where($"minsal" > 2000).show

    spark.close()

  }

}
