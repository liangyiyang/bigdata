package com.lysoft.datastream.transform

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * 将2个流合并成1个流，流的数据类型可以不一样，且一次只能连接2个流。
 */
object ConnectedCoMapTransform {

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

    //连接流只是把2个流的数据放到一个流里面，内部依然保持各自的数据和形式不发生任何变化。
    //优点:灵活度高, 连接的流数据类型可以不同. 缺点:只能连接2个流,要连接3个流需要coMap之后再connect,多个流连接可以使用union
    val connectedSteam: ConnectedStreams[SensorReading, SensorReading] = high.connect(low)
    val coMap: DataStream[Product] = connectedSteam.map(
      warningData => (warningData.id, warningData.temperature, "warning"),
      lowData => (lowData.id, lowData.temperature)
    )

    //打印输出
    coMap.print("coMap:").setParallelism(1)

    //启动
    env.execute("ConnectedCoMapTransform")
  }

}
