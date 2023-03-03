package com.lysoft.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 功能说明：使用Keyed State中的ListState状态，实现两条流的join操作。
 * author:liangyy
 * createtime：2023-03-03 14:07:10
 */
public class TwoStreamsJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                  Tuple3.of("a", "stream1", 1000L)
                , Tuple3.of("b", "stream1", 2000L)
                , Tuple3.of("a", "stream1", 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                  Tuple3.of("a", "stream2", 4000L)
                , Tuple3.of("b", "stream2", 5000L)
                , Tuple3.of("a", "stream2", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })
        );

        stream1.connect(stream2)
                .keyBy(r1 -> r1.f0, r2 -> r2.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    // 定义状态，保存两条流已到达的所有数据
                    private ListState<Tuple2<String, Long>> stream1ListState;
                    private ListState<Tuple2<String, Long>> stream2ListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream1ListState", Types.TUPLE(Types.STRING, Types.LONG)));
                        this.stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("stream2ListState", Types.TUPLE(Types.STRING, Types.LONG)));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取另外一条流所有数据，匹配输出
                        for (Tuple2<String, Long> right : stream2ListState.get()) {
                            out.collect(left.f0 + " " + left.f2 + " => " + right);
                        }

                        this.stream1ListState.add(Tuple2.of(left.f0, left.f2));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取另外一条流所有数据，匹配输出
                        for (Tuple2<String, Long> left : stream1ListState.get()) {
                            out.collect(left + " => " + right.f0 + " " + right.f2);
                        }

                        this.stream2ListState.add(Tuple2.of(right.f0, right.f2));
                    }
                })
                .print();

        env.execute();
    }

}
