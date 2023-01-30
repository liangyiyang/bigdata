package com.lysoft.chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Tuple简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 *
 * sum/min/max函数返回的是最早第一次接收到的那条记录，聚合字段的值则为sum/min/max结果对应的值。
 * minBy函数返回的是指定聚合字段最小的那条记录，maxBy函数返回的是指定聚合字段最大的那条记录。
 */
public class TransformTupleAggregationTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Tuple3<String, Integer, Integer>> streamSource = env.fromElements(
                Tuple3.of("a", 200, 3),
                Tuple3.of("a", 100, 1),
                Tuple3.of("b", 100, 3),
                Tuple3.of("b", 200, 4)
        );

        KeyedStream<Tuple3<String, Integer, Integer>, String> keyedStream = streamSource.keyBy(f -> f.f0);

//        keyedStream.sum(2).print();
        keyedStream.sum("f2").print();
//        keyedStream.max(2).print();
//        keyedStream.max("f2").print();
//        keyedStream.min(2).print();
//        keyedStream.min("f2").print();
//        keyedStream.maxBy(2).print();
//        keyedStream.maxBy("f2").print();
//        keyedStream.minBy(2).print();
//        keyedStream.minBy("f2").print();

        env.execute(TransformTupleAggregationTest.class.getSimpleName());
    }

}
