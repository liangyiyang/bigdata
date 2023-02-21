package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 功能说明：测试自定义AggregateFunction实现窗口增量聚合
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowAggregatePvUvTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 每2秒计算最近10秒钟用户平均的访问次数
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
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();

        env.execute();
    }

    /**
     * 自定义AggregateFunction增量聚合函数、聚合pv和uv
     * AggregateFunction泛型参数：
     * 第一个参数IN是数据流输入的数据类型
     * 第二个参数ACC是聚合结果状态的数据类型
     * 第三个参数OUT是聚合结果输出的数据类型
     */
    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            // 创建初始化1个累加器，只调用一次该方法
            return new Tuple2<>(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
            // 添加用户到HashSet，对用户进行去重
            accumulator.f1.add(value.getUser());
            // 增量计算，每来一条记录调用一次，更新聚合状态中间结果值并返回
            return new Tuple2<>(accumulator.f0 + 1L, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            // 触发窗口计算，返回计算结果
            return Double.valueOf(accumulator.f0 / accumulator.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
            // 合并窗口数据，只有Session会话窗口才需要合并，其他窗口可以不实现该方法
            return null;
        }
    }

}
