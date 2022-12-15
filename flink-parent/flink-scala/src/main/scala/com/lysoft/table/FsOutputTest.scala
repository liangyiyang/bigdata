package com.lysoft.table

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

object FsOutputTest {

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

    //将流转换成表，字段名称和样例类的字段名称一致，则使用的是名称映射，否则是位置映射
    val sensroTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts, 'temperature as 'temp)
    val resultTable: Table = sensroTable
      .select('id, 'temp, 'ts)
      .filter('id === "sensor_1")

    resultTable.toAppendStream[Row].print("tableAPI")

    //将结果输出到文件
    tableEnv.connect(new FileSystem().path("F:\\ideaworkspace\\bigdata\\flink1.7\\src\\main\\resources\\output.txt"))
        .withFormat(new Csv())
        .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("temp", DataTypes.DOUBLE())
            .field("ts", DataTypes.BIGINT())
        ).createTemporaryTable("outputTable")

    resultTable.insertInto("outputTable")

    env.execute(this.getClass.getSimpleName)
  }

}
