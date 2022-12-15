package com.lysoft.datastream.sink

import com.lysoft.datastream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 将数据写入redis
 */
object RedisSink {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合构建数据
    val collectionDStream: DataStream[SensorReading] = env.fromCollection(
      List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444),
        SensorReading("sensor_1", 1547718206, 40.80018327300259),
        SensorReading("sensor_10", 1547718208, 50.101067604893444)
      ))

    //redis配置
    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("172.18.206.7").setPort(8999).setPassword("new@password@hsrj.").build()

    //写入redis
    collectionDStream.addSink(new RedisSink[SensorReading](jedisPoolConfig, new MyRedisMapper()))

    //启动
    env.execute("RedisSink")
  }

}

/**
 * 定义redis的操作
 */
class MyRedisMapper extends RedisMapper[SensorReading] {
  //定义操作redis的命令和HSet的key
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  //定义hset元素的key
  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  //定义hset元素的key对应的value
  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}
