package com.lysoft.flink.wc

import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文本读取数据
    val inputPath: String = "D:\\ideaworkspace\\study\\bigdata\\flink1.7\\src\\main\\resources\\wc.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    //分词之后，对单词进行groupBy分组，然后使用sum聚合
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split("\\s+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印输出
    wordCountDataSet.print()
  }

}
