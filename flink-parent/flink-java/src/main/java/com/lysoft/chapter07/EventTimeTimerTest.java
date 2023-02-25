package com.lysoft.chapter07;

import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 功能说明：测试EventTimeTimer
 * author:liangyy
 * createtime：2023-02-25 21:50:10
 */
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new CustomSource());
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(data -> data.getUser())
                .process(new KeyedProcessFunction<String, Event, String>() {

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        long currentTimestamp = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + "数据到达，时间戳：" + new Timestamp(currentTimestamp) + " watermark：" + ctx.timerService().currentWatermark());

                        // 注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(currentTimestamp + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp) + " watermark：" + ctx.timerService().currentWatermark());
                    }

                }).print();

        env.execute();
    }


    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿5秒钟
            Thread.sleep(5000L);

            // 发出10秒后的数据
            ctx.collect(new Event("Alice", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10秒+1ms后的数据
            ctx.collect(new Event("Bob", "./home", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {
        }
    }

}
