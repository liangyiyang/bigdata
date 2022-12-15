package com.lysoft.processfunction

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFuntionTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    env.setParallelism(1)

    val stream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncrementAlert())

    dataStream.print("input data")
    processedStream.print("processed data")

    env.execute("ProcessFuntionTest")
  }

}

class TempIncrementAlert extends KeyedProcessFunction[String, SensorReading, String] {

  //保存传感器上一次的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  //上一次注册定时器的时间
  lazy val lastRegisterTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastRegisterTime", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //传感器上一次的温度值
    val preTemp: Double = lastTemp.value()
    //更新传感器的温度值
    lastTemp.update(value.temperature)
    //上一次注册的定时器
    val lastTimer: Long = lastRegisterTime.value()

    if (value.temperature > preTemp && lastTimer == 0) {
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      lastRegisterTime.update(timerTs)
    } else if (preTemp > value.temperature || preTemp == 0.0) {
      ctx.timerService().deleteProcessingTimeTimer(lastTimer)
      lastRegisterTime.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "传感器1秒内温度连续上升.")
    lastRegisterTime.clear()
  }

}
