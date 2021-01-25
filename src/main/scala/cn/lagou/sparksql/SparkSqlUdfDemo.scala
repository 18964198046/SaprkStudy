package cn.lagou.sparksql

import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

object SparkSqlUdfDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    var sc = spark.sparkContext
    sc.setLogLevel("warn")

    import spark.implicits._
    val data = List(
      ("scala", "author1"),
      ("scala", "author2"),
      ("hadoop", "author3"),
      ("hive", "author4"),
      ("storm", "author5"),
      ("kafka", "author6"))
    val df = data.toDF("title", "author")
    df.createTempView("books")

    // 定义scala函数并注册
    def len1(str: String): Int = str.length
    spark.udf.register("len1", len1 _)

    // 使用udf,select,where子句
    spark.sql("select title, author, len1(title) as titleLength from books").show()
    spark.sql("select title, author from books where len1(title) > 5").show()

    // DSL
    df.filter("len1(title) > 5").show
    df.select("len1(title)").show
    df.select("title", "author", "len1(title)").show

    import org.apache.spark.sql.functions._
    val len2: UserDefinedFunction = udf(len1 _)
    df.select($"title", $"author", len2($"title")).show
    df.filter(len2($"title") > 5).show

    // 不是用UDF
    df.map{case Row(title: String, author: String) => (title, author, title.length)}

    spark.stop()

  }

}
