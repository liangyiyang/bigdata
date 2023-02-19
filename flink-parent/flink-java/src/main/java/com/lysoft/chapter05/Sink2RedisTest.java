package com.lysoft.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 功能说明：测试sink算子，将数据写入redis。
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class Sink2RedisTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 读取文件数据
        DataStreamSource<Event> dataStreamSource = env.addSource(new ClickSource());

        //3. 构建redis sink
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPassword("123456").build();

        RedisSink<Event> redisSink = new RedisSink<>(flinkJedisPoolConfig, new RedisMapper<Event>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                //定义redis操作命令，第二个参数不是必填参数，集合类型要指定集合名称
                return new RedisCommandDescription(RedisCommand.HSET, "clicks");
                //return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Event event) {
                //写入的redis key，没办法设置key失效时间
                return event.getUser();
                //return "clicks:user:" + event.getUser();
            }

            @Override
            public String getValueFromData(Event event) {
                //写入的redis value
                return event.getUrl();
            }
        });

        //4. 写入redis
        dataStreamSource.addSink(redisSink);

        env.execute();
    }

}
