package com.lysoft.flink.datastream.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSource {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //添加自定义数据源
    val customerSourceStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

    //打印输出
    customerSourceStream.print("customerSourceStream").setParallelism(1)

    //启动
    env.execute("CustomSource")
  }

}

/**
 * 自定义Source数据源
 */
class MySensorSource extends RichSourceFunction[SensorReading] {

  //是否产生数据
  var isRunning: Boolean = _
  var random: Random = _
  var curTemp: IndexedSeq[SensorReading] = _

  override def open(parameters: Configuration): Unit = {
    random = new Random
    isRunning = true
    //初始化10个传感器数据
    curTemp = 1.to(10).map(i =>
      SensorReading(
        "sensor_" + i,
        System.currentTimeMillis(),
        65 + random.nextGaussian() * 20)
    )
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    while (isRunning) {
      //更新传感器温度数据
      curTemp.foreach(sensor => {
        sensor.timestamp = System.currentTimeMillis()
        sensor.temperature = sensor.temperature + random.nextGaussian()
        ctx.collect(sensor)
      })

      Thread.sleep(500)
    }
  }

  //设置不产生数据
  override def cancel(): Unit = {
    isRunning = false
  }

}
