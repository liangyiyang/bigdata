package com.lysoft.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 功能说明：测试使用connect连接两条流
 * author:liangyy
 * createtime：2023-02-26 16:50:10
 */
public class ConnectedStreamTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Long> stream2 = env.fromElements(5L, 6L, 7L, 8L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);
        connectedStreams.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer：" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long：" + value;
            }
        }).print();

        env.execute();
    }

}
