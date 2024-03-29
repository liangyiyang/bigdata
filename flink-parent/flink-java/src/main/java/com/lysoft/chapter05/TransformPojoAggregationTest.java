package com.lysoft.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试POJO类型简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 *
 * min/max函数返回的是最早第一次接收到的那条记录，聚合字段的值则为min/max结果对应的值。
 * minBy函数返回的是指定聚合字段最小值的整条记录，maxBy函数返回的是指定聚合字段最大值的整条记录。
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

        //3. 调用keyBy算子之后才可以使用聚合函数，keyBy是一个逻辑分区，相同数据分发到同一个分区中，数据分发可能会有数据倾斜
        KeyedStream<Event, String> keyedStream = stream.keyBy((KeySelector<Event, String>) key -> key.getUser());

        // 根据指定的属性名进行聚合

        keyedStream.max("timestamp").print("max:");
        keyedStream.maxBy("timestamp").print("maxBy:");

        keyedStream.min("timestamp").print("min:");
        keyedStream.minBy("timestamp").print("minBy:");

        env.execute();
    }

}
