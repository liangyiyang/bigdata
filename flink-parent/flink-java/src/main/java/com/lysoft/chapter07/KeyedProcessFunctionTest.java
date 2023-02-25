package com.lysoft.chapter07;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import com.lysoft.chapter06.UrlViewCount;
import com.lysoft.chapter06.UrlViewCountExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能说明：测试KeyedProcessFunction实现TopN
 * author:liangyy
 * createtime：2023-02-25 21:50:10
 */
public class KeyedProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 先求出每个Url的访问次数
        SingleOutputStreamOperator<UrlViewCount> urlViewCountStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                )
                .keyBy(data -> data.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountExample.UrlViewCountAgg(), new UrlViewCountExample.UrlViewCountResult());

        // 对同一个时间窗口的所有Url访问次数进行排序输出TopN
        urlViewCountStream.keyBy(data -> data.getWindowEnd())
                .process(new TopNKeyedProcessFunction(5))
                .print();

        env.execute();
    }

    /**
     * 自定义KeyedProcessFunction实现TopN
     */
    public static class TopNKeyedProcessFunction extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private int topN;

        private ListState<UrlViewCount> urlViewCountListState;

        public TopNKeyedProcessFunction(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-view-count-list", UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将数据添加到ListState中
            urlViewCountListState.add(value);

            // 注册定时器，输出计算结果
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlViewCount> urlViewCountList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountList.add(urlViewCount);
            }

            // 对list进行降序排序
            urlViewCountList.sort((o1, o2) -> (int) (o2.getViewCount() - o1.getViewCount()));

            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间: " + new Timestamp(ctx.getCurrentKey())).append("\n");

            int size = urlViewCountList.size() <= topN ? urlViewCountList.size() : topN;

            // 取TopN
            for (int i = 0; i < size; i++) {
                UrlViewCount urlViewCount = urlViewCountList.get(i);
                sb.append("No." + (i + 1)).append(" ")
                        .append("Url：" + urlViewCount.getUrl()).append(" ")
                        .append("访问量：" + urlViewCount.getViewCount()).append("\n");
            }

            sb.append("----------------------------------------\n");
            out.collect(sb.toString());
        }

        @Override
        public void close() throws Exception {
            // 清除状态数据
            urlViewCountListState.clear();
        }
    }

}
