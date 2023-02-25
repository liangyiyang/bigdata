package com.lysoft.chapter07;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 功能说明：测试ProcessFucntion
 * author:liangyy
 * createtime：2023-02-25 21:50:10
 */
public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        ).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if ("Mary".equals(value.getUser())) {
                    out.collect(value.getUser());
                } else if ("Bob".equals(value.getUser())) {
                    out.collect(value.getUser());
                    out.collect(value.getUser());
                }

                out.collect(value.toString());
                System.out.println("timestamp = " + ctx.timestamp());
                System.out.println("watermark = " + ctx.timerService().currentWatermark());
                System.out.println("currentProcessingTime = " + ctx.timerService().currentProcessingTime());
                System.out.println("------------------------------------------------------------------------------");
            }
        }).print();

        env.execute();
    }

}
