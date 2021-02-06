package cn.lagou.sparkstream.basic

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}

import scala.util.Random

object SocketServerLikeNC {

  def main(args: Array[String]): Unit = {

    val words: Array[String] = "Hadoop Spark Kafka Hive Zookeeper Hbase Flume Sqoop".split("\\s+")
    val length: Int = words.length
    val port: Int = 9999
    val random: Random = scala.util.Random

    val server = new ServerSocket(port)
    val socket: Socket = server.accept()
    println("成功连接到本地主机: " + socket.getInetAddress)

    while (true) {
      val out = new PrintWriter(socket.getOutputStream)
      out.println(words(random.nextInt(length)) + " " + words(random.nextInt(length)))
      out.flush()
      Thread.sleep(100)
    }
  }

}
