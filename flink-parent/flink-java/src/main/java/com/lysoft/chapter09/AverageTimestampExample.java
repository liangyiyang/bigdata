package com.lysoft.chapter09;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

/**
 * 功能说明：使用Keyed State中的AggregatingState状态，统计5个数据用户的平均访问时长。
 * author:liangyy
 * createtime：2023-03-03 14:07:10
 */
public class AverageTimestampExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.print("input");

        stream.keyBy(data -> data.getUser())
                .flatMap(new AvgTimestampResult(5L))
                .print();

        env.execute();
    }

    public static class AvgTimestampResult extends RichFlatMapFunction<Event, String> {

        // 计数窗口大小
        private Long countSize;

        // 计数器累计次数
        private ValueState<Long> counter;

        // 保存聚合状态
        private AggregatingState<Event, Long> aggregatingState;

        public AvgTimestampResult(Long countSize) {
            this.countSize = countSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.counter = getRuntimeContext().getState(new ValueStateDescriptor<>("counter", Long.class));
            this.aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>(
                    "aggregatingState"
                    , new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                        }
                    }
                    ,Types.TUPLE(Types.LONG, Types.LONG)
            ));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 增量聚合
            this.aggregatingState.add(value);

            // 更新计数器
            Long count = Optional.ofNullable(this.counter.value()).orElse(0L);
            this.counter.update(count + 1);

            // 计数器达到计数窗口大小，输出结果，清空状态
            if (this.counter.value().equals(this.countSize)) {
                out.collect(value.getUser() + "过去" + this.countSize + "次平均访问时间戳：" + this.aggregatingState.get());
                this.aggregatingState.clear();
                this.counter.clear();
            }
        }

    }

}
