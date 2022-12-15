package com.lysoft.datastream.transform

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 将多个流合并成1个流，union的流数据类型要相同，一次可以union多个流。
 */
object UnionTransform {

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

    val unionStream: DataStream[SensorReading] = high.union(low)

    //打印输出
    unionStream.print("unionStream:").setParallelism(1)

    //启动
    env.execute("UnionTransform")
  }

}
