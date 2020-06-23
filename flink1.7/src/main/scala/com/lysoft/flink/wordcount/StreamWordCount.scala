package com.lysoft.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 统计单词次数
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //从命令行获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = params.getRequired("host")
    val port: Int = params.getRequired("port").toInt

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.disableOperatorChaining()

    //从scoket接收文本流
    val textDStream: DataStream[String] = env.socketTextStream(hostname, port)
    val dataStream: DataStream[(String, Int)] = textDStream.flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //打印输出
    dataStream.print().setParallelism(1)

    //启动executor, 执行任务
    env.execute("Socket stream word count job.")
  }

}
