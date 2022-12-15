package com.lysoft.table

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = {
        t.timestamp * 1000L
      }
    })

    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    // 1. Table API
    // 1.1 Group Window聚合操作
    val resultTable: Table = sensorTable
      .window( Tumble over 10.second on 'ts as 'tw )
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.start, 'tw.end)

    //resultTable.toRetractStream[Row].print("resultTable")

    // 1.2 Over Window 聚合操作
    val overResultTable: Table = sensorTable
      .window( Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow )
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow )

    //overResultTable.toRetractStream[Row].print("overResultTable")



    //将流注册成一张表
    tableEnv.createTemporaryView("sensor", sensorTable)
    // 2. SQL实现
    // 2.1 Group Windows
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id), hop_end(ts, interval '4' second, interval '10' second) hw
        |from sensor
        |group by id, hop(ts, interval '4' second, interval '10' second)
        |""".stripMargin)

    //resultSqlTable.toRetractStream[Row].print("resultSqlTable")


    // 2.2 Over Window
    val orderSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, ts, count(id) over w, avg(temperature) over w
        |from sensor
        |window w as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)

    orderSqlTable.toRetractStream[Row].print("orderSqlTable")

    env.execute(this.getClass.getSimpleName)
  }

}
