package com.lysoft.chapter09;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Optional;

/**
 * 功能说明：使用Keyed State中的MapState状态，实现模拟窗口，统计10秒钟每个URL的访问次数。
 * author:liangyy
 * createtime：2023-03-03 14:07:10
 */
public class FakeWindowExample {

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

        stream.keyBy(data -> data.getUrl())
                .process(new FakeWindowResult(Time.seconds(10)))
                .print();

        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        // 窗口长度
        private Long windowSize;

        // 定义状态，保存每个窗口URL的访问次数
        private MapState<Long, Long> mapState;

        public FakeWindowResult(Time windowSize) {
            this.windowSize = windowSize.toMilliseconds();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
            long windowStart = value.getTimestamp() / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册定时器，窗口关闭时输出结果
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态，增量聚合统计pv
            Long pv = Optional.ofNullable(mapState.get(windowStart)).orElse(0L);
            mapState.put(windowStart, pv + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 窗口结束时间
            long windowEnd = timestamp + 1;
            // 窗口开始时间
            long windowStart = windowEnd - windowSize;

            // 输出计算结果
            Long pv = mapState.get(windowStart);
            out.collect("URL：" + ctx.getCurrentKey() + " => " + "窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + " 的访问次数是：" + pv);
            // 模拟窗口销毁， 清空对应窗口的状态
            this.mapState.remove(windowStart);
        }
    }

}
