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

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 功能说明：测试AggregateFunction增量聚合函数和ProcessWindowFunction全窗口聚合函数结合使用
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowAggregateAndProcessWindowUvTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 统计每5秒钟访问的UV
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
                .aggregate(new UvAgg(), new UvCountResult()) // 将增量聚合结果，当作全窗口函数的输入参数，包装窗口信息进行输出
                .print();

        env.execute();
    }

    /**
     * 自定义AggregateFunction增量聚合函数，聚合uv
     * AggregateFunction泛型参数：
     * 第一个参数IN是数据流输入的数据类型
     * 第二个参数ACC是聚合结果状态的数据类型
     * 第三个参数OUT是聚合结果输出的数据类型
     */
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            // 创建初始化1个累加器，只调用一次该方法
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            // 添加用户到HashSet，对用户进行去重
            accumulator.add(value.getUser());
            // 增量计算，每来一条记录调用一次，更新聚合状态中间结果值并返回
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            // 触发窗口计算，返回计算结果
            return Long.valueOf(accumulator.size());
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            // 合并窗口数据，只有Session会话窗口才需要合并，其他窗口可以不实现该方法
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
    public static class UvCountResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            // 窗口开始时间
            long start = context.window().getStart();
            // 窗口结束时间
            long end = context.window().getEnd();

            // uv
            Long uv = elements.iterator().next();

            out.collect("窗口【" + new Timestamp(start) + " ~ " + new Timestamp(end) + "】的UV值是：" + uv);
        }
    }

}
