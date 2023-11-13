package com.lysoft.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * 功能说明：自定义表函数
 * author:liangyy
 * createtime：2023-11-02 16:20:10
 */
public class UdfTestTableFunction {

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
        tableEnv.createTemporarySystemFunction("MySplit", MySplitFunction.class);

        // 3. 调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, url, word, length " +
                " from clickTable, LATERAL TABLE( MySplit(url) ) AS T(word, length)");

        // 4. 转换成流打印输出
        tableEnv.toDataStream(resultTable).print();

        env.execute();
    }

    // 实现自定义表函数
    public static class MySplitFunction extends TableFunction<Tuple2<String, Integer>> {

        public void eval(String str) {
            String[] fields = str.split("\\?");
            for (String field : fields) {
                collect(Tuple2.of(field, field.length()));
            }
        }

    }

}
