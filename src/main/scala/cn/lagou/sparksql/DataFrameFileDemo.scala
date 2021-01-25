package cn.lagou.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataFrameFileDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    // parquet
    val df1: DataFrame = spark.read.load("/data/users.parquet")
    df1.createOrReplaceTempView("t1")
    df1.show

    spark.sql(
      """
        | create or replace temporary view users
        | using parquet
        | options (path "/data/users.parquet")
        |""".stripMargin)

    spark.sql(
      """
        | select * from users
        |""".stripMargin)

    df1.write.mode("overwrite").save("/data/parquet")

    // json
    val df2: DataFrame = spark.read.json("/data/emp.json")
    df2.show(false)

    spark.sql(
      """
        |create or replace temporary view emp
        |using json
        |options (path "/data/emp.json")
        |""".stripMargin)

    spark.sql("select * from emp")
      .write.format("json")
      .mode("overwrite")
      .save("/data/json")

    // csv
    val df3 = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .option("inferschema", true)
      .csv("/data/person.csv")
    df3.show()

    spark.sql(
      """
        |create or replace temporary view person
        |using csv
        |options (path "/data/person.csv",
        |         header "true",
        |         inferschema "true")
        |""".stripMargin)

    spark.sql("select * from person")
      .write.format("csv")
      .mode("overwrite")
      .save("/data/csv")

    // jdbc
    val df4: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://linux123:3306/ebiz?useSSL=false")
      .option("user", "hive")
      .option("password", "12345678")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("query", "select * from lagou_product_info limit 10")
      .load()

    df4.show(10, false)

    df4.write.format("jdbc")
      .option("url", "jdbc:mysql://linux123:3306/ebiz?useSSL=false&characterEncoding=utf-8")
      .option("user", "hive")
      .option("password", "12345678")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "lagou_product_info_back")
      .mode(SaveMode.Append)
      .save

    spark.close()

  }

}
