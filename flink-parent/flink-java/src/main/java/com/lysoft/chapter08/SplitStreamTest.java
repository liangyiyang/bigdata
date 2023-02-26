package com.lysoft.chapter08;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 功能说明：测试使用测输出流实现分流功能
 * author:liangyy
 * createtime：2023-02-26 14:50:10
 */
public class SplitStreamTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("maryTag") {};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("bobTag") {};

        SingleOutputStreamOperator<Event> processedStream = env.addSource(new ClickSource())
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                        if ("Mary".equals(value.getUser())) {
                            ctx.output(maryTag, new Tuple3<>(value.getUser(), value.getUser(), value.getTimestamp()));
                        } else if ("Bob".equals(value.getUser())) {
                            ctx.output(bobTag, new Tuple3<>(value.getUser(), value.getUser(), value.getTimestamp()));
                        } else {
                            out.collect(value);
                        }
                    }
                });

        processedStream.print("else");
        processedStream.getSideOutput(maryTag).print("Mary");
        processedStream.getSideOutput(bobTag).print("Bob");

        env.execute();
    }

}
