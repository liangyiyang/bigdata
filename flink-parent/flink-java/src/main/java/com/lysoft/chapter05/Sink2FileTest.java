package com.lysoft.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 功能说明：测试sink算子，将数据写入文件。
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class Sink2FileTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //2. 构建数据
        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        //3. 构建文件参数
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                    new Path("./output"), new SimpleStringEncoder<>("UTF-8")
                ).withRollingPolicy(
                     //定义文件滚动策略
                     DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10)) //10分钟触发滚动
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //5分钟没有数据写入触发滚动
                        .withMaxPartSize(1024 * 1024 * 128) //达到文件大小触发滚动 128M
                        .build()
                ).build();

        //4. 将Event转换成String写入文件
        dataStreamSource.map(data -> data.toString()).addSink(fileSink);

        env.execute();
    }

}
