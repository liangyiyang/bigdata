package com.lysoft.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 功能说明：统计单词次数
 * author:liangyy
 * createtime：2022-12-14 09:45:48
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文件
        DataSource<String> dataSource = env.readTextFile(BatchWordCount.class.getResource("/words.txt").getPath());

        //3. 将单词组装成元组类型
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split("\\s");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4. 按单词分组
        UnsortedGrouping<Tuple2<String, Long>> groupBy = flatMapOperator.groupBy(0);

        //5. 按单词统计单词次数
        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        //6. 打印结果
        sum.print();
    }

}
