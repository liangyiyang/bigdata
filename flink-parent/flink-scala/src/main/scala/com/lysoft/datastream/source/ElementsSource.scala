package com.lysoft.datastream.source

import org.apache.flink.streaming.api.scala._

object ElementsSource {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合构建数据
    val elementsStream: DataStream[Any] = env.fromElements("frank", 172, 60, 30)

    //打印输出
    elementsStream.print("elementsStream:").setParallelism(1)

    //启动
    env.execute("ElementsSource")
  }

}
