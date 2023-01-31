package com.lysoft.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能说明：统计单词次数
 * author:liangyy
 * createtime：2022-12-14 09:45:48
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromArgs(args);
        String hostname = pt.get("hostname");
        int port = pt.getInt("port");

        //2. 读取文件
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        //3. 将单词组装成元组类型
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = dataStreamSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split("\\s");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 按单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = streamOperator.keyBy((KeySelector<Tuple2<String, Long>, String>) key -> key.f0);
        //KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = streamOperator.keyBy(data -> data.f0);

        //5. 按单词统计单词次数
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        //6. 打印结果
        sum.print();

        //7. 执行
        env.execute();
    }

}
