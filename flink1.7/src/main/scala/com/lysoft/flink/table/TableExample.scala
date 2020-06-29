package com.lysoft.flink.table

import com.lysoft.flink.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableExample {

  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //从文本读取数据
    val inputPath: String = getClass.getResource("/sensor.txt").getPath
    val fileStream: DataStream[String] = env.readTextFile(inputPath)

    //将数据映射为样例类
    val dataStream: DataStream[SensorReading] = fileStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //2. 基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //3. 基于tableEnv，将流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream)
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    //3.1 把表转换成流，打印输出
    resultTable.toAppendStream[(String, Double)].print("tableAPI")


    //4. 基于tableEnv，将流转换成表，使用SQL查询
    tableEnv.createTemporaryView("sensorTable", dataStream)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from sensorTable
        |where id = 'sensor_1'
        |""".stripMargin)

    //4.1 把表转换成流，打印输出
    resultSqlTable.toAppendStream[Row].print("sqlAPI")

    env.execute(this.getClass.getSimpleName)
  }

}
