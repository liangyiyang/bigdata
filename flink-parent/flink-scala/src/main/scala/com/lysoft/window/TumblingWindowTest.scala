package com.lysoft.window

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 分析执行过程，输入如下数据
 * 第一个窗口输入数据：
 * sensor_1, 1547718199, 35.80018327300259
 * sensor_6, 1547718201, 15.402984393403084
 *
 * 输入到第二条记录，窗口关闭了，输出结果为：min data> (sensor_1,35.80018327300259)
 * 计算窗口起始和结束时间为：
 * 1、当前输入记录事件时间为1547718201，程序设置数据延迟一秒到达，wartermark水位认为1547718200之前的数据已经到达，10秒一个窗口，
 * 所以计算出窗口的起始时间为1547718190、结束时间为1547718200，下一个窗口为1547718200 ~ 1547718210，因为延迟一秒，输入1547718211才会关闭窗口。
 * 2、窗口数据是左闭右开原则，即包含开始时间，不含结束时间的数据。
 *
 *
 * 第二个窗口输入数据：
 * sensor_7, 1547718202, 6.720945201171228
 * sensor_10, 1547718205, 38.101067604893444
 * sensor_1, 1547718206, 35.1
 * sensor_1, 1547718207, 35.6
 * sensor_1,1547718209,10.2
 * sensor_1,1547718210,20.6
 * sensor_1,1547718211,20.6
 *
 * 事件时间输入到10秒后窗口关闭了
 * 输出结果为：
 * min data> (sensor_6,15.402984393403084)
 * min data> (sensor_1,10.2)
 * min data> (sensor_10,38.101067604893444)
 * min data> (sensor_7,6.720945201171228)
 */
object TumblingWindowTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    env.setParallelism(1)

    //设置周期性生成wartermark的间隔时间
    env.getConfig.setAutoWatermarkInterval(200)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val inputPath: String = getClass.getResource("/sensor.txt").getPath
    //val stream: DataStream[String] = env.readTextFile(inputPath)
    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(
      //延迟1秒
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          //指定EventTime时间语义字段
          t.timestamp * 1000L
        }
    })

    //统计每个传感器10秒钟内最低的温度
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //窗口数据是左闭右开原则，即包含开始时间，不含结束时间的数据
      .timeWindow(Time.seconds(10))
      .reduce( (data1, data2) => {
        (data1._1, data1._2.min(data2._2))
      } )

    minTempPerWindowStream.print("min data")
    dataStream.print("input data")

    env.execute("window test")
  }

}
