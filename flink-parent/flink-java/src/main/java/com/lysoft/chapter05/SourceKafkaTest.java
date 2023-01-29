package com.lysoft.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 功能说明：测试FlinkKafkaSource算子
 * author:liangyy
 * createtime：2022-12-28 09:45:48
 */
public class SourceKafkaTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group-01");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //2. 从kafka消费数据
        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties));

        clicks.print();

        env.execute(SourceKafkaTest.class.getSimpleName());
    }

}
