package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import com.sun.javaws.jnl.RContentDesc;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 功能说明：测试ProcessWindowFunction全窗口函数
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowProcessWindowFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 统计每10秒钟访问的UV
        stream.assignTimestampsAndWatermarks(
                        // 有序的数据流
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                )
                .keyBy(data -> "allKey")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new UvCountByWindow()) // 全窗口函数，等收集完窗口所有数据之后才会关闭窗口，触发计算
                .print();

        env.execute();
    }

    /**
     * 自定义ProcessWindowFunction处理函数
     * ProcessWindowFunction泛型参数
     * 第一个参数IN是数据流输入的数据类型
     * 第二个参数OUT是聚合结果输出的数据类型
     * 第三个参数KEY是KeyBy中key的数据类型
     * 第三个参数W是Window
     */
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        /**
         * @param key The key for which this window is evaluated.
         * @param context The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out A collector for emitting elements.
         * @throws Exception
         */
        @Override
        public void process(String key, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 保存用户
            HashSet<String> hashSet = new HashSet<>();
            for (Event event : elements) {
                // 用hashset对用户进行去重
                hashSet.add(event.getUser());
            }

            //窗口的开始时间
            long start = context.window().getStart();
            //窗口的结束时间
            long end = context.window().getEnd();

            out.collect("窗口【" + new Timestamp(start) + " ~ " + new Timestamp(end) + "】的UV值是：" + hashSet.size());
        }
    }

}
