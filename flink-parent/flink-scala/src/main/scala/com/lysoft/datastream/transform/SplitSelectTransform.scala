package com.lysoft.datastream.transform

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 根据某个条件，将一个流切分成多个流，分别对数据流进行不同的处理
 */
object SplitSelectTransform {

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

    //切分流，已过时，Please use side output instead.
    val splitSream: SplitStream[SensorReading] = collectionDStream.split(sensorData => if (sensorData.temperature > 30) Seq("high") else Seq("low"))

    val high: DataStream[SensorReading] = splitSream.select("high")
    val low: DataStream[SensorReading] = splitSream.select("low")
    val all: DataStream[SensorReading] = splitSream.select("high", "low")

    //打印输出
    high.print("high:").setParallelism(1)
    low.print("low:").setParallelism(1)
    all.print("all:").setParallelism(1)

    //启动
    env.execute("SplitSelectTransform")
  }

}
