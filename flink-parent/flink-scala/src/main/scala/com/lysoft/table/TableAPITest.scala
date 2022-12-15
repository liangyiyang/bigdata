package com.lysoft.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

object TableAPITest {

  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2. 基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    /*// 1.1 老版本planner的流式查询
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //用老版本planner
      .inStreamingMode() //流处理模式
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 老版本批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    // 1.3 blink版本的流式查询
    val blinkSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() //用新版本blink的planner
      .inStreamingMode() //流处理模式
      .build()
    val blinkTableEnv = StreamTableEnvironment.create(env, blinkSettings)

    // 1.4 blink版本的批式查询
    // blink将批处理当成一种特殊的流来处理，因此都是当成datastream来处理的
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)*/


    //从文本读取数据
    val inputPath: String = this.getClass.getResource("/sensor.txt").getPath

    //定义读取外部文件
    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv()) // 指定从外部文件读取数据方式
      .withSchema(
        //定义表结构
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable") // 在表环境注册一张表


    //定义读取kafka消息
    tableEnv.connect(
      new Kafka()
        .version("0.11") //定义版本
        .topic("hsrj_user_search") //定义主题
        .property("bootstrap.servers", "172.18.59.202:9092,172.18.59.203:9092,172.18.59.204:9092")
        .property("group.id", "CID_alikafka_hsrj_flume")
        .property("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .property("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .property("auto.offset.reset", "latest")
    )
      .withFormat(new Json)
      .withSchema(new Schema()
        .field("user_id", DataTypes.BIGINT())
        .field("search_keyword", DataTypes.STRING())
        .field("action_time", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaInputTable")

    //使用tableAPI
    val sensorTable: Table = tableEnv.from("inputTable")
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    //使用tableAPI 聚合查询
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    resultTable.toAppendStream[Row].print("tableAPI")
    aggResultTable.toRetractStream[Row].print("aggTable")


    //使用SQL
    val aggResultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id, count(id) cnt
        |from inputTable
        |group by id
        |""".stripMargin)

    aggResultSqlTable.toRetractStream[Row].print("agg sqlAPI")

    val kafkaTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from kafkaInputTable
        |""".stripMargin)
    kafkaTable.toAppendStream[Row].print("kafkaTable")

    env.execute(this.getClass.getSimpleName)
  }

}
