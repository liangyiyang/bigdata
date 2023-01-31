package com.lysoft.chapter05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试归约聚合算子
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class TransformReduceTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());

        //每条记录算一次访问次数
        SingleOutputStreamOperator<Tuple2<String, Long>> tupleStream = clickStream.map(data -> Tuple2.of(data.getUser(), 1L)).returns(new TypeHint<Tuple2<String, Long>>() {});

        //求每个用户的访问次数
        SingleOutputStreamOperator<Tuple2<String, Long>> userViewCountStream = tupleStream.keyBy(data -> data.f0).reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        //求访问次数最多的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = userViewCountStream.keyBy(data -> "key").reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> (value1.f1 > value2.f1 ? value1 : value2));

        result.print();

        env.execute();
    }

}
