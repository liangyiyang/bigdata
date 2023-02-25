package com.lysoft.chapter07;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * 功能说明：测试windowAll全窗口和ProcessAllWindowFunction全窗口函数
 * author:liangyy
 * createtime：2023-02-25 21:50:10
 */
public class ProcessAllWindowFunctionTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                )
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult(3))
                .print();


        env.execute();
    }

    /**
     * 增量统计Url的访问次数，利用HashMap实现区分Url。
     */
    public static class UrlHashMapCountAgg implements AggregateFunction<Event, HashMap<String, Integer>, List<Tuple2<String, Integer>>> {

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(Event value, HashMap<String, Integer> accumulator) {
            Integer urlViewCount = Optional.ofNullable(accumulator.get(value.getUrl())).orElse(0);
            accumulator.put(value.getUrl(), urlViewCount + 1);
            return accumulator;
        }

        @Override
        public List<Tuple2<String, Integer>> getResult(HashMap<String, Integer> accumulator) {
            List<Tuple2<String, Integer>> result = new ArrayList<>();
            for (String key : accumulator.keySet()) {
                result.add(new Tuple2<>(key, accumulator.get(key)));
            }

            // 对list进行降序排序
            result.sort((o1, o2) -> o2.f1 - o1.f1);

            return result;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
            return null;
        }
    }

    /**
     * 用全窗口函数包装输出信息
     */
    public static class UrlAllWindowResult extends ProcessAllWindowFunction<List<Tuple2<String, Integer>>, String, TimeWindow> {

        private int topN;

        public UrlAllWindowResult(int topN) {
            this.topN = topN;
        }

        @Override
        public void process(ProcessAllWindowFunction<List<Tuple2<String, Integer>>, String, TimeWindow>.Context context, Iterable<List<Tuple2<String, Integer>>> elements, Collector<String> out) throws Exception {
            List<Tuple2<String, Integer>> list = elements.iterator().next();

            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间: " + new Timestamp(context.window().getEnd())).append("\n");

            int size = list.size() <= topN ? list.size() : topN;

            // 取TopN
            for (int i = 0; i < size; i++) {
                Tuple2<String, Integer> element = list.get(i);
                sb.append("No." + (i + 1)).append(" ")
                        .append("Url：" + element.f0).append(" ")
                        .append("访问量：" + element.f1).append("\n");
            }

            sb.append("----------------------------------------\n");
            out.collect(sb.toString());
        }
    }

}
