package com.lysoft.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 功能说明：自定义聚合函数
 * author:liangyy
 * createtime：2023-11-02 16:20:10
 */
public class UdfTestAggregateFunction {

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
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 3. 调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, WeightedAverage(ts, 1) from clickTable group by user");

        // 4. 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // 自定义一个累加器类型
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // 实现自定义聚合函数，计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {

        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            }

            return accumulator.sum / accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加器方法
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }

    }

}
