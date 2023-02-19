package com.lysoft.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：自定义Source算子
 * author:liangyy
 * createtime：2022-12-28 09:45:48
 */
public class SourceCustomTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度为1
        env.setParallelism(1);

        //2. 添加自定义source
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print();

        env.execute();
    }

}
