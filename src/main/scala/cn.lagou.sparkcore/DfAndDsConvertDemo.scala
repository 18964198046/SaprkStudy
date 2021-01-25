package cn.lagou.sparkcore

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object DfAndDsConvertDemo {

  case class Person(name:String, age:Int, height:Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext;
    sc.setLogLevel("warn")

    import spark.implicits._
    val arr = Array(("Jack", 28, 150),("Tom", 10, 144), ("Andy", 16, 165))
    val rdd = sc.makeRDD(arr).map(f => Person(f._1, f._2, f._3))
    val ds = rdd.toDS()
    val df = rdd.toDF()
    ds.printSchema()
    df.printSchema()
    ds.orderBy(desc("name")).show(10)
    ds.orderBy(desc("name")).show(10)

    spark.close()
  }

}
