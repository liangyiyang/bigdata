package com.lysoft.chapter07;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 功能说明：测试ProcessingTimeTimer
 * author:liangyy
 * createtime：2023-02-25 21:50:10
 */
public class ProcessingTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(data -> data.getUser())
                .process(new KeyedProcessFunction<String, Event, String>() {

                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        long currentTimestamp = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + "数据到达，到达时间：" + new Timestamp(currentTimestamp));

                        // 注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currentTimestamp + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp));
                        System.out.println("-------------------------------------------------------------------");
                    }

                }).print();

        env.execute();
    }

}
