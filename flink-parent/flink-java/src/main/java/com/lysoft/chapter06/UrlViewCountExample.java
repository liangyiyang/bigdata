package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 功能说明：测试AggregateFunction增量聚合函数和ProcessWindowFunction全窗口聚合函数结合使用
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class UrlViewCountExample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置多少毫秒的周期性生成Watermark
        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 统计5秒钟每个Url的访问次数
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
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

    /**
     * 自定义AggregateFunction增量聚合函数，聚合Url访问次数
     * AggregateFunction泛型参数：
     * 第一个参数IN是数据流输入的数据类型
     * 第二个参数ACC是聚合结果状态的数据类型
     * 第三个参数OUT是聚合结果输出的数据类型
     */
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    /**
     * 自定义ProcessWindowFunction 包装窗口信息输出
     * ProcessWindowFunction泛型参数
     * 第一个参数IN是增量聚合函数输出结果的数据类型
     * 第二个参数OUT是聚合结果输出的数据类型
     * 第三个参数KEY是KeyBy中key的数据类型
     * 第三个参数W是TimeWindow
     */
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 窗口的开始时间
            long start = context.window().getStart();
            // 窗口的结束时间
            long end = context.window().getEnd();

            // url访问次数
            Long viewCount = elements.iterator().next();

            // 输出结果
            out.collect(new UrlViewCount(key, viewCount, start, end));
        }

    }
}
