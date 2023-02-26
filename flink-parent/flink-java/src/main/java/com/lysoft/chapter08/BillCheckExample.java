package com.lysoft.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 功能说明：测试使用connect连接两条流，实现对账单
 * author:liangyy
 * createtime：2023-02-26 16:50:10
 */
public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // app支付事件
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                        new Tuple3<>("order-1", "app", 1000L),
                        new Tuple3<>("order-2", "app", 2000L),
                        new Tuple3<>("order-3", "app", 3500L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }));

        // 第三方支付事件
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                new Tuple4<>("order-1", "third-party", "success", 3000L),
                new Tuple4<>("order-3", "third-party", "success", 4000L),
                new Tuple4<>("order-4", "third-party", "success", 4500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                }));


        // 先keyBy再连接，是一种方式
/*        appStream.keyBy(data -> data.f0)
                .connect(thirdPartyStream.keyBy(data -> data.f0));*/

        // 先连接再keyBy，等价第一种keyBy方式，内部实现就是两条流分别keyBy之后返回ConnectedStreams
        appStream.connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        // app支付事件状态
        private ValueState<Tuple3<String, String, Long>> appEventState;

        // 第三方支付事件状态
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
            thirdPartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (thirdPartyEventState.value() != null) {
                out.collect("对账成功：" + value + " " + thirdPartyEventState.value());
                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);

                // 注册一个5秒的定时器，等到5秒后还没有第三方支付事件，则输出告警
                ctx.timerService().registerEventTimeTimer(value.f2 + 5 * 1000);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null) {
                out.collect("对账成功：" + appEventState.value() + " " + value);
                // 清空状态
                appEventState.clear();
            } else {
                // 更新状态
                thirdPartyEventState.update(value);

                // 注册一个5秒的定时器，等到5秒后还没有app支付事件，则输出告警
                ctx.timerService().registerEventTimeTimer(value.f3);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " " + "第三方支付平台信息未到");
            }

            if (thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + " " + "app支付信息未到");
            }

            thirdPartyEventState.clear();
            appEventState.clear();
        }

        @Override
        public void close() throws Exception {
            appEventState.clear();
            thirdPartyEventState.clear();
        }

    }

}
