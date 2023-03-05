package com.lysoft.chapter09;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能说明：使用Operator State中的ListState状态，缓存批量输出结果
 * author:liangyy
 * createtime：2023-03-05 14:07:10
 */
public class BufferingSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        stream.print("input");

        // 缓存批量输出
        stream.addSink(new BufferingSinkFunction(10));

        env.execute();
    }

    public static class BufferingSinkFunction implements SinkFunction<Event>, CheckpointedFunction {

        // 缓存的记录条数
        private int bufferSize;

        // 缓存的数据
        private List<Event> bufferedRecords;

        // 缓存的数据，用于flink将缓存数据进行checkpoint持久化
        private ListState<Event> checkpointedListState;

        public BufferingSinkFunction(int bufferSize) {
            this.bufferSize = bufferSize;
            this.bufferedRecords = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            // 将数据添加到列表缓存
            this.bufferedRecords.add(value);

            // 如果列表缓存达到缓存大小，批量输出结果
            if (this.bufferedRecords.size() == this.bufferSize) {
                for (Event record : this.bufferedRecords) {
                    System.out.println(record);
                }
                System.out.println("==========================输出完毕==========================");
                this.bufferedRecords.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空上一次checkpoint保存的状态数据，避免重复
            this.checkpointedListState.clear();

            // 将列表缓存数据复制到状态列表中以便flink进行checkpoint，把算子状态数据持久化
            this.checkpointedListState.addAll(this.bufferedRecords);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> listStateDescriptor = new ListStateDescriptor<>("recordListState", Event.class);
            this.checkpointedListState = context.getOperatorStateStore().getListState(listStateDescriptor);

            // 如果是从checkpoint检查点中恢复数据，将ListState的所有数据复制到列表缓存
            if (context.isRestored()) {
                for (Event event : this.checkpointedListState.get()) {
                    this.bufferedRecords.add(event);
                }
            }
        }
    }

}
