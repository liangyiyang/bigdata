package com.lysoft.chapter06;

import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 功能说明：测试flink内置Watermark生成
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WatermarkTest {

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
                // 有序流的Watermark生成
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event element, long recordTimestamp) {
//                        // 抽取事件时间
//                        return element.getTimestamp();
//                    }
//                }));

                // 乱序流的Watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                // 抽取事件时间
                                return element.getTimestamp();
                            }
                        }));

        stream
//              .windowAll(GlobalWindows.create()) // 非KeyedStream的全局窗口只有1个并行度，不推荐使用，计算性能低。
                .keyBy(data -> data.getUser()) // keyBy之后的开窗为KeyedStream，按照key进行开窗，每1个key对应1个窗口，同一个key如果跨多个时间窗口，则该key会有多个时间窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 事件时间-滚动窗口
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 事件时间-滑动窗口
//                .window(EventTimeSessionWindows.withGap(Time.minutes(30))) // 事件时间-会话窗口

//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 处理时间-滚动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 处理时间-滑动窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(30))) // 处理时间-会话窗口

//                .countWindow(10) // 计数-滚动窗口
//                .countWindow(10, 5) // 计数-滑动窗口
                .windowAll(GlobalWindows.create()) // 全局窗口，keyBy之后的全局窗口，每个key全局只有1个窗口，全局窗口没有时间的概念，不会触发计算，不会关闭窗口，必须定义触发器(Trigger)执行计算。该全局窗口与非KeyedStream的全局窗口区别，非KeyedStream的全局窗口只有1个并行度。
                ;

        env.execute();
    }

}
