package com.lysoft.flink.datastream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object Source4Custom {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val customerSourceStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

    //打印输出
    customerSourceStream.print("customerSourceStream").setParallelism(1)

    //启动
    env.execute("Source4Custom")
  }

}

/**
 * 自定义Source
 */
class MySensorSource extends SourceFunction[SensorReading] {

  //表示是否产生数据
  var isRunning: Boolean = true;

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random: Random = new Random()

    val curTemp = 1.to(10).map(i => SensorReading("sensor_" + i, System.currentTimeMillis(), 65 + random.nextGaussian() * 20))

    while (isRunning) {
      curTemp.foreach(sensor => ctx.collect(SensorReading(sensor.id, System.currentTimeMillis(), sensor.temperature + random.nextGaussian())))
      Thread.sleep(500)
    }
  }

  //设置不产生数据
  override def cancel(): Unit = {
    isRunning = false
  }

}
