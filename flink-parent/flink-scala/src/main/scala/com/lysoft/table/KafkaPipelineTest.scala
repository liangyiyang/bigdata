package com.lysoft.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}

object KafkaPipelineTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //用老版本planner
      .inStreamingMode() //流处理模式
      .build()
    //基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //定义kafka的连接，创建输入表
    tableEnv.connect(new Kafka()
      .version("0.11") //定义版本
      .topic("hsrj_user_search") //定义主题
      .property("bootstrap.servers", "172.18.59.202:9092,172.18.59.203:9092,172.18.59.204:9092")
      .property("group.id", "CID_alikafka_hsrj_flume")
      .property("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .property("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .property("auto.offset.reset", "latest")
    ) .withFormat(new Json)
      .withSchema(new Schema()
        .field("user_id", DataTypes.BIGINT())
        .field("search_keyword", DataTypes.STRING())
        .field("action_time", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaInputTable")


    //定义kafka的连接，创建输出表
    tableEnv.connect(new Kafka()
      .version("0.11") //定义版本
      .topic("hsrj_user_search_pv_uv") //定义主题
      .property("bootstrap.servers", "172.18.46.204:9092")
    ) .withFormat(new Json)
      .withSchema(new Schema()
        .field("user_id", DataTypes.BIGINT())
        .field("search_keyword", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaOutputTable")

    //将数据输出到kafka
    tableEnv.sqlUpdate(
      """
        |insert into kafkaOutputTable
        |select user_id, search_keyword
        |from kafkaInputTable
        |""".stripMargin)

    env.execute(this.getClass.getSimpleName)
  }

}
