package com.lysoft.flink.datastream.source

import org.apache.flink.streaming.api.scala._

object Source4File {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从文件读取数据
    val fileStream: DataStream[String] = env.readTextFile("D:\\ideaworkspace\\study\\bigdata\\flink1.7\\src\\main\\resources\\sensor.txt")

    //打印输出
    fileStream.print("fileStream:").setParallelism(1)

    //启动
    env.execute("Source4File")
  }

}
