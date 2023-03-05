package com.lysoft.chapter09;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

/**
 * 功能说明：测试Keyed State状态
 * author:liangyy
 * createtime：2023-03-03 14:07:10
 */
public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 启用检查点
        env.enableCheckpointing(10000);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.keyBy(data -> data.getUser())
                .flatMap(new MyFlatMap())
                .print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {

        // 定义值状态
        private ValueState<Event> valueState;

        // 定义列表状态
        private ListState<Event> listState;

        // 定义map状态
        private MapState<String, Long> mapState;

        // 定义归约聚合状态
        private ReducingState<Event> reducingState;

        // 定义聚合状态
        private AggregatingState<Event, String> aggregatingState;

        // 定义本地变量
        private Long count = 0L;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 设置状态的失效时间
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("valuestate", Event.class);
            valueStateDescriptor.enableTimeToLive(ttlConfig);
            this.valueState = getRuntimeContext().getState(valueStateDescriptor);
            this.listState = getRuntimeContext().getListState(new ListStateDescriptor<>("liststate", Event.class));
            this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapstate", String.class, Long.class));
            this.reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(
                    "reducingState"
                    , new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.getUser(), value1.getUrl(), value2.getTimestamp());
                        }
                    }
                    , Event.class
            ));
            this.aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>(
                    "aggregatingState"
                    , new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }

                        @Override
                        public String getResult(Long accumulator) {
                            return accumulator.toString();
                        }

                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }
                    , Long.class
            ));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            valueState.update(value);
            System.out.println("valueState：" + value.getUser() + " " + valueState.value());

            listState.add(value);
            System.out.println("listState：" + value.getUser() + " " + listState);

            Long viewCount = Optional.ofNullable(mapState.get(value.getUser())).orElse(0L);
            mapState.put(value.getUser(), viewCount + 1);
            System.out.println("mapState：" + value.getUser() + " " + mapState.get(value.getUser()));

            reducingState.add(value);
            System.out.println("reducingState：" + reducingState.get());

            aggregatingState.add(value);
            System.out.println("aggregatingState：" + aggregatingState.get());

            count++;
            // count是统计所有消息的，跟keyBy没有关系
            System.out.println("count：" + count);

            System.out.println("---------------------------------------------------------------------------");

        }

    }

}
