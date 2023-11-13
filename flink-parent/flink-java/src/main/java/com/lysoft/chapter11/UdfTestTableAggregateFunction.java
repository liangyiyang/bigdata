package com.lysoft.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能说明：自定义表聚合函数
 * author:liangyy
 * createtime：2023-11-02 16:20:10
 */
public class UdfTestTableAggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        tableEnv.executeSql("create table clickTable(" +
                "  `user` string" +
                " ,url string" +
                " ,ts bigint" +
                " ,et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000))" +
                " ,WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.csv'," +
                " 'format' = 'csv'" +
                ")");

        // 2. 注册自定义表函数
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // 3. 调用UDF进行查询转换
        String windowAggQuery = "select user" +
                "  ,count(*) url_cnt" +
                "  ,window_start" +
                "  ,window_end" +
                " from TABLE(" +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                " )" +
                " group by user, window_start, window_end";

        Table aggTable = tableEnv.sqlQuery(windowAggQuery);

        Table resultTable = aggTable.groupBy($("window_end"))
                .flatAggregate(call("Top2", $("url_cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        // 4. 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // 自定义一个累加器类型
    public static class Top2Accumulator {
        // top1第一最大值
        public Long first;

        // top2第二最大值
        public Long second;
    }

    // 实现自定义表聚合函数，实现Top2
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator accumulator = new Top2Accumulator();
            accumulator.first = Long.MIN_VALUE;
            accumulator.second = Long.MIN_VALUE;
            return accumulator;
        }

        // 定义一个更新累加器的方法
        public void accumulate(Top2Accumulator accumulator, Long value) {
            if (value > accumulator.first) {
                accumulator.second = accumulator.first;
                accumulator.first = value;
            } else if (value > accumulator.second) {
                accumulator.second = value;
            }
        }

        // 输出结果，当前的top2
        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Long, Integer>> out) {
            if (accumulator.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.first, 1));
            } else if (accumulator.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.second, 2));
            }
        }
    }
}
