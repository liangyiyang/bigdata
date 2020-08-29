package com.lysoft.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //设置conf
    val conf: SparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[1]")

    //创建sc
    val sc: SparkContext = new SparkContext(conf)

    //读取文件
    val line: RDD[String] = sc.textFile("D:\\ideaworkspace\\study\\bigdata\\spark-learning\\spark-core\\src\\main\\resources\\word.txt")

    //将字符串切分为单词
    val words: RDD[String] = line.flatMap(_.split("\\s+"))

    //将单词和1放到元组
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //根据单词进行聚合
    val wordCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //根据单词出现的次数降序排序
    val result: RDD[(String, Int)] = wordCount.sortBy(_._2, false)

    //打印单词统计次数
    result.foreach(println(_))

    //关闭连接
    sc.stop()
  }

}
