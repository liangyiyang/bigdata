package com.lysoft.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.security.Key;

/**
 * 功能说明：测试Filter转换算子
 * author:liangyy
 * createtime：2022-12-28 20:38:10
 */
public class TransformFilterTest {

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

//        SingleOutputStreamOperator<Event> filterStream = stream.filter((FilterFunction<Event>) event -> event.getUser().contains("Mary"));
//        SingleOutputStreamOperator<Event> filterStream = stream.filter(new KeyWordFilter("Bob"));

        // true保留记录，false丢弃记录
        SingleOutputStreamOperator<Event> filterStream = stream.filter((FilterFunction<Event>) event -> event.getUser().contains("Alice"));

        filterStream.print();

        env.execute();
    }

    /**
     * 自定义FilterFunction算子
     */
    public static class KeyWordFilter implements FilterFunction<Event> {

        private String keyword;

        public KeyWordFilter(String keyword) {
            this.keyword = keyword;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            return value.getUser().contains(this.keyword);
        }

    }
}
