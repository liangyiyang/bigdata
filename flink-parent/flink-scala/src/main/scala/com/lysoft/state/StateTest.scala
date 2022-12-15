package com.lysoft.state

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object StateTest {

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
        .keyBy(_.id)
        .flatMapWithState[(String, Double, Double), Double] {
          case (input: SensorReading, None) => (List.empty, Some(input.temperature))
          case (input: SensorReading, lastTemp: Some[Double]) =>
            //两次的温度差值
            val diff = (input.temperature - lastTemp.get).abs
            //两次温度差值超过阈值则告警
            if (diff > 10.0) {
              (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
            } else {
              (List.empty, Some(input.temperature))
            }
        }
        //.flatMap(new TempChangeAlert2(10.0))
        //.process(new TempChangeAlert(10.0))

    dataStream.print("input data")
    processedStream.print("processed data")

    env.execute("sate test")
  }

}

class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  //传感器上一次的温度值
  lazy private val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    //上一次的温度
    val lastTemp = lastTempState.value()
    //两次的温度差值
    val diff = (value.temperature - lastTemp).abs
    //两次温度差值超过阈值则告警
    if (diff > threshold && lastTemp > 0) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }

}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  //传感器上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //上一次的温度
    val lastTemp = lastTempState.value()
    //两次的温度差值
    val diff = (value.temperature - lastTemp).abs
    //两次温度差值超过阈值则告警
    if (diff > threshold && lastTemp > 0) {
      out.collect((value.id, lastTemp, value.temperature))
    }
    lastTempState.update(value.temperature)
  }
}
