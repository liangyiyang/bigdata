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
        // 设置全局并行度为1
        env.setParallelism(1);

        //2. 添加自定义source
        DataStreamSource<Event> stream = env.addSource(new ParallelClickSource()).setParallelism(2);

        stream.print();

        env.execute();
    }

    /**
     * 自定义有多并行度的SourceFunction
     */
    public static class ParallelClickSource implements ParallelSourceFunction<Event> {

        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
            Random random = new Random();

            while (isRunning) {
                ctx.collect(new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis()));
                // 隔1秒生成一个点击事件，方便观测
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }

}
