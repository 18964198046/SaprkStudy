package cn.lagou.sparkcore

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcDemo {
  def main(args: Array[String]): Unit = {
    val str = "hadoop spark java hbase hive sqoop hue tez atlas datax grinffin zk kafka"
    val result: Array[(String, Int)] = str.split("\\s+").zipWithIndex

    val url = "jdbc:mysql://linux123:3306/ebiz?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val username = "hive"
    val password = "12345678"

    var conn: Connection = null
    var stat: PreparedStatement = null
    val sql = "insert into `wordcount` (`word`, `count`) values (?, ?)"
    try {
      conn = DriverManager.getConnection(url, username, password)
      stat = conn.prepareStatement(sql)
      result.foreach{case (k,v) =>
        stat.setString(1, k)
        stat.setInt(2, v)
        stat.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stat != null) stat.close()
      if (conn != null) conn.close()
    }
  }
}
