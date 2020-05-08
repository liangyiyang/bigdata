package com.lysoft.flink.datastream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lysoft.flink.datastream.source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * 将数据写入MySQL
 */
object Sink2MySQL {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合构建数据
    val collectionDStream: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_1", 1547718206, 40.80018327300259),
      SensorReading("sensor_10", 1547718208, 50.101067604893444)
    ))

    //写入MySQL
    collectionDStream
      //keyBy之后再写入，避免多个线程同时写入主键冲突问题，或者设置sink的并行度为1
      .keyBy(_.id)
      .addSink(new MySQLJDBCSink())
      //设置sink的并行度为1, 避免多个线程同时写入主键冲突问题
      //.setParallelism(1)

    //启动
    env.execute("Sink2MySQL")
  }

}

/**
 * 自定义sink，写入MySQL数据库
 * create table t_temperature(sensor varchar(20) not null, temperature double not null);
 */
class MySQLJDBCSink extends RichSinkFunction[SensorReading] {

  var connection: Connection = _
  var updateStmt: PreparedStatement = _
  var insertStmt: PreparedStatement = _

  /**
   * 初始化数据库连接
   * 每个并行度会执行一次初始化操作
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    println("init connection...")

    connection = DriverManager.getConnection("jdbc:mysql://172.18.206.11:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "hsrj@123")
    updateStmt = connection.prepareStatement("update t_temperature set temperature = ? where sensor = ?")
    insertStmt = connection.prepareStatement("insert into t_temperature(sensor, temperature) values(?, ?)")
  }

  /**
   * 每条记录会执行一次
   *
   * @param value
   * @param context
   */
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    println("invoke ...")

    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    val updateCount: Int = updateStmt.executeUpdate()

    //没有数据更新,则插入数据
    if (updateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.executeUpdate()
    }
  }

  /**
   * 关闭数据连接
   * 每个并行度会执行一次关闭操作
   */
  override def close(): Unit = {
    println("close connection...")

    if (updateStmt != null) {
      updateStmt.close()
    }

    if (insertStmt != null) {
      insertStmt.close()
    }

    if (connection != null) {
      connection.close()
    }
  }

}
