package com.lysoft.chapter06;

import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能说明：测试自定义Watermark生成
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WatermarkCustomGeneratorTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<Event> stream = env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Alice", "./prod?id=200", 3500L),
                        new Event("Bob", "./prod?id=2", 2500L),
                        new Event("Alice", "./prod?id=300", 3600L),
                        new Event("Bob", "./home", 3000L),
                        new Event("Bob", "./prod?id=1", 2300L),
                        new Event("Bob", "./prod?id=3", 3300L))
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        // 返回自定义的Watermark生成器
                        return new CustomPeriodicGenerator(Duration.ofSeconds(2));
                    }

                    @Override
                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                // 抽取事件时间
                                return element.getTimestamp();
                            }
                        };
                    }
                });

        stream.print();

        env.execute();
    }

    /**
     * 自定义周期性生成Watermark
     */
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        //当前最大Watermark
        private long maxTimestamp;

        //数据延迟时间
        private final long outOfOrdernessMillis;

        public CustomPeriodicGenerator(Duration maxOutOfOrderness) {
            checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
            checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //每一条记录都会调用此方法
            //保存当前最大时间戳作为Watermark，Watermark只能递增，不能回退。
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {
            //周期性发出Watermark，默认200ms生成一次Watermark
            //发出Watermark，真正的Watermark值为当前最大时间 - 数据延迟时间 - 1毫秒，因为窗口是左闭右开，不包含Watermark水位线的值，所以要减去1毫秒。
            out.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }


    /**
     * 自定义断点式生成Watermark
     */
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput out) {
            //只有在遇到特定的条件时，才发出水位线
            if (event.getUser().equals("Alice")) {
                out.emitWatermark(new Watermark(event.getTimestamp() - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //不需要做任何事情，已在onEvent方法中发出了水位线
        }
    }

}


