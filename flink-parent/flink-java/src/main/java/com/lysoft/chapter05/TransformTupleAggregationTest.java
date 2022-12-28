package com.lysoft.chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Tuple简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 */
public class TransformTupleAggregationTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从元素读取数据
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamSource.keyBy(f -> f.f0);

//        keyedStream.sum(1).print();
//        keyedStream.sum("f1").print();
//        keyedStream.max(1).print();
//        keyedStream.max("f1").print();
//        keyedStream.min(1).print();
//        keyedStream.min("f1").print();
//        keyedStream.maxBy(1).print();
//        keyedStream.maxBy("f1").print();
//        keyedStream.minBy(1).print();
        keyedStream.minBy("f1").print();

        env.execute();
    }

}
