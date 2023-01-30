package com.lysoft.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Pojo简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 *
 * min/max函数返回的是最早第一次接收到的那条记录，聚合字段的值则为min/max结果对应的值。
 * minBy函数返回的是指定聚合字段最小的那条记录，maxBy函数返回的是指定聚合字段最大的那条记录。
 */
public class TransformPojoAggregationTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./fav", 4000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./prod?id=1", 3000L)
        );

        KeyedStream<Event, String> keyedStream = stream.keyBy((KeySelector<Event, String>) key -> key.getUser());

        keyedStream.max("timestamp").print("max:");
        keyedStream.maxBy("timestamp").print("maxBy:");

        keyedStream.min("timestamp").print("min:");
        keyedStream.minBy("timestamp").print("minBy:");

        env.execute(TransformPojoAggregationTest.class.getSimpleName());
    }

}
