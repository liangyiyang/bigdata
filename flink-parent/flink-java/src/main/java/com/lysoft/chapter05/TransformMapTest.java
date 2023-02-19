package com.lysoft.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Map转换算子
 * author:liangyy
 * createtime：2022-12-28 20:38:10
 */
public class TransformMapTest {

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

        // 映射转换
        SingleOutputStreamOperator<String> mapStream = stream.map(event -> event.getUser());
        //SingleOutputStreamOperator<String> mapStream = stream1.map((MapFunction<Event, String>) event -> event.getUser());
        //SingleOutputStreamOperator<String> mapStream = stream1.map(new UserExtractor());

        mapStream.print();

        env.execute();
    }

    /**
     * 自定义MapFunction算子
     */
    public static class UserExtractor implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.getUser();
        }

    }

}
