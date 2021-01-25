package cn.lagou.sparkcore

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ReadCsvToDataFrame {

  case class Person(name:String, age:Integer, job:String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val df1: DataFrame = spark.read
      .option("header", "true") // 包含列名
      .option("delimiter", ",") // 分隔符
      .option("inferschema", "true") // 自动类型推断
      .csv("/data/person.csv")
    df1.printSchema()
    df1.show()

    val schema = "name string, age int, job string"
    val df2: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .csv("/data/person.csv")
    df2.printSchema()
    df2.show()

    import spark.implicits._
    val ds1 = df1.as[Person]
    ds1.printSchema();
    ds1.show()

  }

}
