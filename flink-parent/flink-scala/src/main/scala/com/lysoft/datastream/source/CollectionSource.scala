package com.lysoft.datastream.source

import org.apache.flink.streaming.api.scala._

object CollectionSource {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合构建数据
    val collectionDStream: DataStream[SensorReading] = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444),
        SensorReading("sensor_1", 1547718206, 40.80018327300259),
        SensorReading("sensor_10", 1547718208, 50.101067604893444)
    ))

    //打印输出
    collectionDStream.print("collectionDStream:").setParallelism(1)

    //启动
    env.execute("CollectionSource")
  }

}
