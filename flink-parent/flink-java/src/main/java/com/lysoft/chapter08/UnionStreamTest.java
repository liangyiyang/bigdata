package com.lysoft.chapter08;

import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 功能说明：测试union合并多条流，watermark水位线以多条流中最小的watermark为准
 * author:liangyy
 * createtime：2023-02-26 14:50:10
 */
public class UnionStreamTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] arr = value.split("\\s+");
                        return new Event(arr[0].trim(), arr[1].trim(), Long.valueOf(arr[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("localhost", 7778)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] arr = value.split("\\s+");
                        return new Event(arr[0].trim(), arr[1].trim(), Long.valueOf(arr[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream1.print("stream1");
        stream2.print("stream2");

        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线：" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }

}
