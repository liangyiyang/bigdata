package com.lysoft.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 功能说明：测试FlatMap转换算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 */
public class TransformFlatMapTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

//        SingleOutputStreamOperator<String> flatMapStream = stream.flatMap(new MyFlatMapFunction());

        // flatMap算子可以输出1条或多条记录
        SingleOutputStreamOperator<String> flatMapStream = stream.flatMap((FlatMapFunction<Event, String>) (event, out) -> {
            if (event.getUser().equals("Mary")) {
                out.collect(event.getUser());
                out.collect(event.getUrl());
            } else {
                out.collect(event.getUser());
                out.collect(event.getUrl());
                out.collect(event.getTimestamp().toString());
            }
        }).returns(new TypeHint<String>() {});

        flatMapStream.print();

        env.execute();
    }

    /**
     * 自定义FlatMapFunction算子
     */
    public static class MyFlatMapFunction implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            if (event.getUser().equals("Mary")) {
                out.collect(event.getUser());
                out.collect(event.getUrl());
            } else {
                out.collect(event.getUser());
                out.collect(event.getUrl());
                out.collect(event.getTimestamp().toString());
            }
        }

    }

}
