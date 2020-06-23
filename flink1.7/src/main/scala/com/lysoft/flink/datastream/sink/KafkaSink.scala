package com.lysoft.flink.datastream.sink

import com.lysoft.flink.datastream.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

/**
 * 将数据写入kafka
 */
object KafkaSink {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合构建数据
    val collectionDStream: DataStream[String] = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259).toString,
        SensorReading("sensor_6", 1547718201, 15.402984393403084).toString,
        SensorReading("sensor_7", 1547718202, 6.720945201171228).toString,
        SensorReading("sensor_10", 1547718205, 38.101067604893444).toString,
        SensorReading("sensor_1", 1547718206, 40.80018327300259).toString,
        SensorReading("sensor_10", 1547718208, 50.101067604893444).toString
      ))

    //写入kafka
    collectionDStream.addSink(new FlinkKafkaProducer010[String]("172.18.206.11:9092", "flink_test", new SimpleStringSchema()))
    //打印输出
    collectionDStream.print("collectionDStream:").setParallelism(1)

    //启动
    env.execute("KafkaSink")
  }

}
