package com.lysoft.wordcount

import org.apache.flink.api.scala._

/**
 * 统计单词次数
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文本读取数据
    val inputPath: String = getClass.getResource("/wc.txt").getPath
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
