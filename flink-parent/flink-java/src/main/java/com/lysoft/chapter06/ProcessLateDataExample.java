package com.lysoft.chapter06;

import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 功能说明：测试处理延迟数据
 * author:liangyy
 * createtime：2023-02-23 17:50:10
 */
public class ProcessLateDataExample {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置多少毫秒的周期性生成Watermark
        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);

        // 定义测输出流
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        // 统计5秒钟每个Url的访问次数
        SingleOutputStreamOperator<UrlViewCount> result = stream.map((MapFunction<String, Event>) value -> {
                    String[] arr = value.split("\\,");
                    return new Event(arr[0].trim(), arr[1].trim(), Long.valueOf(arr[2].trim()));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                ).keyBy(data -> data.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) // 允许窗口延迟1分钟关闭，以便接受迟到数据，到了窗口结束时间，会先触发一次计算输出结果，后续1分钟内来的迟到数据每一条数据会触发一次窗口计算。
                .sideOutputLateData(outputTag) // 窗口关闭后，再来迟到的数据，收集到测输流。
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlViewCountResult());

        // 输出统计结果
        result.print("result");

        // 输出迟到数据
        result.getSideOutput(outputTag).print("late");

        // 打印输入数据
        stream.print("input");

        env.execute();
    }

}
