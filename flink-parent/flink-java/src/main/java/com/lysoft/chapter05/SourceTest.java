package com.lysoft.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能说明：测试Source算子
 * author:liangyy
 * createtime：2022-12-28 09:45:48
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 从文件读取数据
        DataStreamSource<String> stream1 = env.readTextFile(SourceTest.class.getResource("/clicks.txt").getPath());

        List<Integer> numList = new ArrayList<>();
        numList.add(1);
        numList.add(2);
        //3. 从集合读取数据-基础数据类型
        DataStreamSource<Integer> numStream = env.fromCollection(numList);

        List<Event> eventList = new ArrayList<>();
        eventList.add(new Event("Mary", "./home", 1000L));
        eventList.add(new Event("Bob", "./cart", 2000L));
        //4. 从集合读取数据-POJO类型
        DataStreamSource<Event> eventStream = env.fromCollection(eventList);

        //5. 从元素读取数据-POJO类型
        DataStreamSource<Event> elementStream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //6. 从socket文本流读取数据
        DataStreamSource<String> socketStream = env.socketTextStream("172.20.108.148", 7777);

        //stream1.print("stream1");
        //numStream.print("numStream");
        //eventStream.print("eventStream");
        //elementStream.print("elementStream");
        socketStream.print("socketStream");

        env.execute();
    }

}
