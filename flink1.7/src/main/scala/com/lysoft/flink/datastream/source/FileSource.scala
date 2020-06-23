package com.lysoft.flink.datastream.source

import org.apache.flink.streaming.api.scala._

object FileSource {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从文本读取数据
    val inputPath: String = getClass.getResource("/sensor.txt").getPath
    val fileStream: DataStream[String] = env.readTextFile(inputPath)

    //打印输出
    fileStream.print("fileStream:").setParallelism(1)

    //启动
    env.execute("FileSource")
  }

}
