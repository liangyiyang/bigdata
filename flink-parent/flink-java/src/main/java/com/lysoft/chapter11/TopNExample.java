package com.lysoft.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能说明：Flink SQL开窗聚合函数
 * author:liangyy
 * createtime：2023-11-02 16:20:10
 */
public class TopNExample {

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

        // 普通Top N，选取当前所有用户中浏览量最大的2个
        Table topNResultTable = tableEnv.sqlQuery("select user" +
                " ,url_cnt" +
                " ,row_num" +
                " from (" +
                "    select ROW_NUMBER() OVER(ORDER BY url_cnt desc) row_num" +
                "         ,user" +
                "         ,url_cnt" +
                "    from (" +
                "       select user, count(*) url_cnt from clickTable group by user" +
                "    )" +
                ") where row_num <= 2");

        tableEnv.toChangelogStream(topNResultTable).print("top 2:");


        String subQuery = "select user" +
                "  ,count(*) url_cnt" +
                "  ,window_start" +
                "  ,window_end" +
                " from TABLE(" +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                " )" +
                " group by user, window_start, window_end";

        //窗口TOP N， 统计一段时间内的（前2名）活跃用户
        Table windowTopNResult = tableEnv.sqlQuery("select user" +
                " ,url_cnt" +
                " ,row_num" +
                " from (" +
                "    select ROW_NUMBER() OVER(PARTITION BY window_start, window_end ORDER BY url_cnt desc) row_num" +
                "         ,user" +
                "         ,url_cnt" +
                "    from (" + subQuery + " )" +
                ") where row_num <= 2");

        tableEnv.toDataStream(windowTopNResult).print("window top 2:");

        env.execute();
    }

}
