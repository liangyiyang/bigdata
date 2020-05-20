package com.lysoft.flink.window

import com.lysoft.flink.datastream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 分析执行过程，输入如下数据
 * 第一个窗口输入数据：
 * sensor_1, 1547718199, 35.8
 * sensor_1, 1547718200, 32.8
 * sensor_6, 1547718201, 15.4
 *
 * 输入到第三条记录，窗口关闭了，输出结果为：min data> (sensor_1,35.8)
 * 计算窗口起始和结束时间为：
 * 1、当前输入记录事件时间为1547718201，程序设置数据延迟一秒到达，wartermark水位认为1547718200之前的数据已经到达，15秒一个窗口，每次滑动5秒
 * 所以计算出窗口的起始时间为1547718185、结束时间为1547718200，下一个窗口为1547718190 ~ 1547718205，每5秒统计一次结果，因为延迟一秒，输入1547718206才会输出计算结果。
 * 2、窗口数据是左闭右开原则，即包含开始时间，不含结算时间的数据。
 *
 *
 * 第二个窗口输入数据：
 * sensor_7, 1547718202, 16.8
 * sensor_7, 1547718203, 26.3
 * sensor_7, 1547718204, 36.5
 * sensor_10, 1547718205, 38.1
 * sensor_1, 1547718206, 35.1
 *
 * 事件时间输入到5秒后窗口关闭了
 * 输出结果为：
 * min data> (sensor_6,15.4)
 * min data> (sensor_7,16.8)
 * min data> (sensor_1,32.8)
 *
 *
 * 第三个窗口输入数据：
 * sensor_1, 1547718207, 18.4
 * sensor_1, 1547718210, 20.6
 * sensor_1, 1547718211, 30.2
 *
 * 事件时间输入到5秒后窗口关闭了
 * 输出结果为：
 * min data> (sensor_1,18.4)
 * min data> (sensor_7,16.8)
 * min data> (sensor_10,38.1)
 * min data> (sensor_6,15.4)
 */
object SlidingWindowTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度
    env.setParallelism(1)

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

    //每5秒统计每个传感器最近15秒钟内最低的温度
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //窗口数据是左闭右开原则，即包含开始时间，不含结算时间的数据
      //.window(new SlidingEventTimeWindows(Time.seconds(15), Time.seconds(5), Time.hours(-8)))
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce( (data1, data2) => {
        (data1._1, data1._2.min(data2._2))
      } )

    minTempPerWindowStream.print("min data")
    dataStream.print("input data")

    env.execute("window test")
  }

}
