package com.lysoft.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Pojo简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 */
public class TransformPojoAggregationTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./prod?id=1", 3000L),
                new Event("Mary", "./fav", 4000L)
        );

        KeyedStream<Event, String> keyedStream = stream.keyBy((KeySelector<Event, String>) key -> key.getUser());

        keyedStream.max("timestamp").print("max:");

        keyedStream.maxBy("timestamp").print("maxBy:");

        env.execute();
    }

}
