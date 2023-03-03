package com.lysoft.chapter09;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

/**
 * 功能说明：使用Keyed State中的ValueState状态，统计每个用户的URL访问次数，每5秒钟才输出一次统计结果。
 * author:liangyy
 * createtime：2023-03-03 14:07:10
 */
public class PeriodicPvExample {

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

        stream.print("input：");

        stream.keyBy(data -> data.getUser())
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    /**
     * 统计每个用户的URL访问次数，每5秒钟才输出一次统计结果，减少输出频率，避免下游写入数据频繁引起性能问题。
     */
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        // 用户的浏览次数
        private ValueState<Long> urlViewCountState;

        // 保存定时器的触发时间，用于判断是否注册过定时器，避免后面数据重复注册，覆盖了之前定时器的触发时间。
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.urlViewCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("urlViewCount", Long.class));
            this.timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            Long urlViewCount = Optional.ofNullable(urlViewCountState.value()).orElse(0L);
            // 更新用户的浏览次数
            urlViewCountState.update(urlViewCount + 1);

            // 判断是否注册过定时器
            if (this.timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 10 * 1000L);
                this.timerTsState.update(value.getTimestamp() + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "的访问次数是：" + this.urlViewCountState.value());

            // 清空这个状态相当于统计10秒窗口的URL访问次数
            //this.urlViewCountState.clear();

            // 清空状态
            this.timerTsState.clear();
        }
    }

}
