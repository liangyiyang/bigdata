package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 功能说明：测试自定义AggregateFunction实现窗口增量聚合
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowAggregateFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 统计每5秒钟用户的平均访问时长
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
                .keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new UserAvgAccessTime())
                .print();

        env.execute();
    }

    /**
     * 自定义AggregateFunction增量聚合函数
     * AggregateFunction泛型参数：
     * 第一个参数IN是数据流输入的数据类型
     * 第二个参数ACC是聚合结果状态的数据类型
     * 第三个参数OUT是聚合结果输出的数据类型
     */
    public static class UserAvgAccessTime implements AggregateFunction<Event, Tuple2<Long, Integer>, String> {

        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            // 创建初始化1个累加器，只调用一次该方法
            return new Tuple2<>(0L, 0);
        }

        @Override
        public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
            // 增量计算，每来一条记录调用一次，更新聚合状态中间结果值并返回
            return new Tuple2<>(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
        }

        @Override
        public String getResult(Tuple2<Long, Integer> accumulator) {
            // 触发窗口计算，返回计算结果
            return new Timestamp(accumulator.f0 / accumulator.f1).toString();
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            // 合并窗口数据，只有Session会话窗口才需要合并，其他窗口可以不实现该方法
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}
