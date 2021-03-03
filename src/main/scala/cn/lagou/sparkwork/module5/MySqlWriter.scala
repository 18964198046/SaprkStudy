package cn.lagou.sparkwork.module5

import java.sql.{Connection, Statement}
import java.util.Properties

import cn.lagou.sparkwork.module5.MySqlWriter.getConnection
import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter


object MySqlWriter {

  private val LOG = Logger.getLogger(MySqlWriter.getClass.getName)

  val dataSource: Option[DataSource] = {
    try {
      val druidProps = new Properties()
      druidProps.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
      druidProps.setProperty("url", "jdbc:mysql://linux123:3306/lg_bus?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      druidProps.setProperty("username", "root")
      druidProps.setProperty("password", "123456")
      druidProps.setProperty("initialSize", "5")
      druidProps.setProperty("maxActive", "10")
      druidProps.setProperty("maxWait", "3000")
      druidProps.setProperty("maxIdle", "8")
      druidProps.setProperty("minIdle", "3")

      // 获取Druid连接池的配置文件
      //val druidConfig = getClass.getResourceAsStream(MySQL_Properties)
      // 倒入配置文件
      //druidProps.load(druidConfig)
      Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        LOG.error("Error Create Mysql Connection", error)
        None
    }
  }

  // 连接方式
  def getConnection: Option[Connection] = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }

}

class MySqlWriter extends ForeachWriter[BusInfo]{

  var statement: Statement  = null
  var connection: Connection = null

  override def open(partitionId: Long, epochId: Long): Boolean = {
    connection = MySqlWriter.getConnection.get
    true;
  }

  override def process(value: BusInfo): Unit = {
    val lgLat: String = value.lglat
    val deployNum: String = value.deployNum
    val sql =  "insert into lg_bus_info (lg_lat, deploy_num) where (" + lgLat + "," + deployNum + ")"
    statement = connection.createStatement()
    statement.executeQuery(sql);
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }

}
