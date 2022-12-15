package com.lysoft.processfunction

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    env.setParallelism(1)

    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(
        //延迟1秒
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(t: SensorReading): Long = {
            //指定EventTime时间语义字段
            t.timestamp * 1000L
          }
        })

    val processedStream = dataStream
        .process(new FreezingAlert())

    processedStream.print("processed data")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("SideOutputTest")
  }

}

/**
 * 定义侧输出流,ProcessFunction第二个泛型是指主输出流的数据类型，侧输出流类型在定义侧输出流时指定数据类型
 */
class FreezingAlert extends ProcessFunction[SensorReading, SensorReading] {

  lazy val alertOutput = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      out.collect(value)
    }
  }

}
