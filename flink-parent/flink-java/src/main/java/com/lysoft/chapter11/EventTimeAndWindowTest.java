package com.lysoft.chapter11;

import com.lysoft.chapter05.ClickSource;
import com.lysoft.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能说明：Table API和SQL的时间语义定义和使用(事件时间)
 * author:liangyy
 * createtime：2023-11-02 10:07:10
 */
public class EventTimeAndWindowTest {

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

        tableEnv.executeSql("select * from clickTable").print();

        // 2. 在流转换成Table时定义时间属性
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").as("ts"), $("et").rowtime());
        //clickTable.printSchema();

        // 聚合查询转换

        // 1. 分组聚合
        Table aggTable = tableEnv.sqlQuery("select user, count(*) url_cnt from clickTable group by user");

        // 2. 分组窗口聚合
        Table groupWindowResultTable = tableEnv.sqlQuery("select user" +
                " ,count(*) url_cnt" +
                " ,TUMBLE_END(et, INTERVAL '10' SECOND) window_end" +
                " from clickTable" +
                " group by user, TUMBLE(et, INTERVAL '10' SECOND)"
        );

        // 3. 窗口聚合
        // 3.1 滚动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("select user" +
                " ,count(*) url_cnt" +
                " ,window_end" +
                " from TABLE(" +
                "   TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                ")" +
                " group by user, window_start, window_end"
        );

        // 3.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("select user" +
                " ,count(*) url_cnt" +
                " ,window_end" +
                " from TABLE(" +
                "  HOP( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND )" +
                ")" +
                "group by user, window_start, window_end"
        );

        // 3.3 累积窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("select user" +
                " ,count(*) url_cnt" +
                " ,window_end" +
                " from TABLE(" +
                "  CUMULATE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND )" +
                ")" +
                "group by user, window_start, window_end"
        );

        // 4. 开窗聚合
        Table overWindowResultTable = tableEnv.sqlQuery("select user" +
                " ,AVG(ts) OVER(PARTITION BY user ORDER BY et ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_ts" +
                " from clickTable");

        // 结果表转换成流打印输出
//        tableEnv.toChangelogStream(aggTable).print("agg: ");
//        tableEnv.toChangelogStream(groupWindowResultTable).print("groupWindowResultTable: ");
//        tableEnv.toChangelogStream(tumbleWindowResultTable).print("tumbleWindowResultTable: ");
//        tableEnv.toChangelogStream(hopWindowResultTable).print("hopWindowResultTable: ");
//        tableEnv.toChangelogStream(cumulateWindowResultTable).print("cumulateWindowResultTable: ");
        tableEnv.toChangelogStream(overWindowResultTable).print("overWindowResultTable: ");


        env.execute();
    }

}
