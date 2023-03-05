package com.lysoft.chapter09;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 功能说明：使用Operator State中的BroadcastState状态，实现动态更新规则配置
 * author:liangyy
 * createtime：2023-03-05 14:07:10
 */
public class BehaviorPatternDetectExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用户的事件行为数据流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "payment"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        // 规则配置流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "payment"),
                new Pattern("login", "order")
        );

        // 定义广播状态描述器
        MapStateDescriptor<Void, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(patternMapStateDescriptor);

        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector());

        matches.print();

        env.execute();
    }

    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 记录用户上一次事件行为
        private ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("preActionState", Types.STRING);
            this.preActionState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 定义广播状态描述器
            MapStateDescriptor<Void, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
            // 从上下文中获取广播状态
            ReadOnlyBroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(patternMapStateDescriptor);

            // 获取配置规则
            Pattern pattern = patternState.get(null);

            if (this.preActionState.value() != null && pattern != null) {
                if (pattern.action1.equals(this.preActionState.value()) && pattern.action2.equals(value.action)) {
                    out.collect(Tuple2.of(value.userId, pattern));
                }
            }

            // 更新用户上一次事件行为
            this.preActionState.update(value.getAction());
        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 定义广播状态描述器
            MapStateDescriptor<Void, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

            // 从上下文中获取广播状态
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(patternMapStateDescriptor);

            // 更新广播状态数据
            patternState.put(null, value);
        }

    }


    /**
     * 用户的行为事件实体类
     */
    public static class Action {
        // 用户ID
        private String userId;

        // 用户事件行为
        private String action;

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }
    }

    /**
     * 定义规则实体类
     */
    public static class Pattern {
        // 用户事件行为1
        private String action1;

        // 用户事件行为2
        private String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        public String getAction1() {
            return action1;
        }

        public void setAction1(String action1) {
            this.action1 = action1;
        }

        public String getAction2() {
            return action2;
        }

        public void setAction2(String action2) {
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }


}
