package com.lysoft.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：测试Rich富函数生命周期
 *         每个并行度open和close方法会被初始化调用一次。
 * author:liangyy
 * createtime：2022-12-29 10:38:10
 */
public class TransformRichFunctionTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 3000L)
        );

        stream.map(new RichMapFunction<Event, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open生命周期被调用：" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
            }

            @Override
            public String map(Event event) throws Exception {
                return event.toString();
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close生命周期被调用：" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
            }

        }).print();

        env.execute(TransformRichFunctionTest.class.getSimpleName());
    }

}
