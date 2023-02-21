package com.lysoft.chapter06;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 功能说明：测试自定义ReduceFunction实现窗口增量聚合
 * author:liangyy
 * createtime：2023-02-18 21:50:10
 */
public class WindowReduceFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置周期性生成Watermark的时间间隔，单位毫秒, 默认200毫毫秒
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("data");

        // 统计每5秒钟用户访问的次数
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
                .map((MapFunction<Event, Tuple2<String, Long>>) event -> new Tuple2<>(event.getUser(), 1L)).returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new UserViewCntReduceFunction())
                .print();

        env.execute();
    }

    /**
     * 自定义ReduceFunction增量聚合函数
     * 该函数限制了中间聚合结果、输入参数、输出结果的数据类型，3者参数类型都要求一致，缺乏灵活性、通用性。
     */
    public static class UserViewCntReduceFunction implements ReduceFunction<Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

}
