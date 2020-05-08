package com.lysoft.flink.datastream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object Source4Kafka {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从kafka读取数据
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "172.18.59.202:9092,172.18.59.203:9092,172.18.59.204:9092")
    props.setProperty("group.id", "CID_alikafka_hsrj_flume")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String]("hsrj_user_search", new SimpleStringSchema(), props))
    //打印输出
    kafkaStream.print("kafkaStream").setParallelism(1)

    //启动
    env.execute("Source4Kafka")
  }

}
