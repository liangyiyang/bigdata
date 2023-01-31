package com.lysoft.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 功能说明：测试sink算子，将数据写入kafka。
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class Sink2KafkaTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 读取文件数据
        DataStreamSource<String> dataStreamSource = env.readTextFile(Sink2KafkaTest.class.getResource("/clicks.csv").getPath());

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");

        //3. 构建kafka sink
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("clicks", new SimpleStringSchema(), properties);

        //4. 写入kafka
        dataStreamSource.addSink(kafkaProducer);

        env.execute();
    }

}
