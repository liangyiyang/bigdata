package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 功能说明：测试自定义ProcessWindowFunction全窗口函数，全窗口函数等收集完窗口所有数据后才会触发计算
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowTriggerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 统计每5秒钟url访问的次数
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                )
                .keyBy(data -> data.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new CalUrlViewCountTrigger())
                .process(new UrlViewCountWindowFunction())
                .print();

        env.execute();
    }

    public static class UrlViewCountWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // url访问次数
            Long viewCount = elements.spliterator().getExactSizeIfKnown();

            // 窗口的开始时间
            long start = context.window().getStart();
            // 窗口的结束时间
            long end = context.window().getEnd();

            String result = "窗口[" + new Timestamp(start) + " ~ " + new Timestamp(end) + "]的url[" + key + "]访问次数是：" + viewCount;
            out.collect(result);
        }
    }

    public static class CalUrlViewCountTrigger extends Trigger<Event, TimeWindow> {

        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(1000L);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

}
