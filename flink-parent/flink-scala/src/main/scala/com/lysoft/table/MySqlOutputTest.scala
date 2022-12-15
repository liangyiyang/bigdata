package com.lysoft.flink.table

import com.lysoft.flink.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

object MySqlOutputTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //用老版本planner
      .inStreamingMode() //流处理模式
      .build()
    //基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //从文本读取数据
    val filePath: String = this.getClass.getResource("/sensor.txt").getPath
    val fileStream: DataStream[String] = env.readTextFile(filePath)

    //将数据映射为样例类
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val dataArr: Array[String] = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    //将流注册成表
    tableEnv.createTemporaryView("sensorTable", dataStream)

    // 统计结果输出到Mysql
    val sinkDDL: String =
      """
        |create table sink_sensor_count (
        | id varchar(20) not null,
        | sensor_cnt bigint not null
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://rm-wz97qwj24gv96in6r.mysql.rds.aliyuncs.com:3306/drgou-bigdata?useUnicode=true&characterEncoding=UTF-8&useSSL=false',
        | 'connector.table' = 'sensor_count',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'drgou_bigdata',
        | 'connector.password' = '1osRNTvV9fNoC1ji'
        |)
     """.stripMargin
    tableEnv.sqlUpdate(sinkDDL)

    val aggResultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id) sensor_cnt
        |from sensorTable
        |group by id
        |""".stripMargin)

    //将结果写入MySQL
    aggResultSqlTable.insertInto("sink_sensor_count")

    env.execute(this.getClass.getSimpleName)
  }
}
