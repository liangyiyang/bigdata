package com.lysoft.chapter05;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Tuple类型简单聚合算子
 * author:liangyy
 * createtime：2022-12-28 20:50:10
 *
 * sum/min/max函数返回的是最早第一次接收到的那条记录，聚合字段的值则为sum/min/max结果对应的值。
 * minBy函数返回的是指定聚合字段最小值的整条记录，maxBy函数返回的是指定聚合字段最大值的整条记录。
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

        //3. 调用keyBy算子之后才可以使用聚合函数，keyBy是一个逻辑分区，相同数据分发到同一个分区中，数据分发可能会有数据倾斜
        KeyedStream<Tuple3<String, Integer, Integer>, String> keyedStream = streamSource.keyBy(f -> f.f0);

        // 根据指定元组的下标位置或元组的属性名进行聚合

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

        env.execute();
    }

}
