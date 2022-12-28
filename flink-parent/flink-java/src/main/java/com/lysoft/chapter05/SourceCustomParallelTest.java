package com.lysoft.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 功能说明：自定义并行Source算子
 * author:liangyy
 * createtime：2022-12-28 09:45:48
 */
public class SourceCustomParallelTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 添加自定义source
        DataStreamSource<Integer> stream = env.addSource(new ParallelClickSource()).setParallelism(2);

        stream.print();

        env.execute();
    }

    public static class ParallelClickSource implements ParallelSourceFunction<Integer> {

        private boolean isRunning = true;

        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(random.nextInt());
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }

}
