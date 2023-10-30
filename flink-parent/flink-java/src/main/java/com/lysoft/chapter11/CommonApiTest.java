package com.lysoft.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能说明：Table API和SQL的使用测试
 * author:liangyy
 * createtime：2023-08-10 10:07:10
 */
public class CommonApiTest {

    public static void main(String[] args) throws Exception {
/*        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);*/

        // 1. 定义环境配置来创建表
        // 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode() // 使用流模式
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

/*        // 1.1 基于老版本planner进行流处理
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode() // 使用流模式
                .useOldPlanner()
                .build();

        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);*/

/*        // 1.2 基于老版本planner进行批处理
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useOldPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);*/

/*        // 1.3 基于blink版本planner进行批处理
        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);*/

        // 2. 创建表
        tableEnv.executeSql("CREATE TABLE clickTable (" +
                " user_name STRING" +
                ",url STRING" +
                ",ts BIGINT" +
                ") WITH(" +
                " 'connector' = 'filesystem'" +
                ",'path' = 'input/clicks.csv'" +
                ",'format' = 'csv'" +
                ")");

        // 3. 表的查询转换
        // 3.1 调用Table API
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob")).select($("user_name"), $("url"));

        tableEnv.createTemporaryView("resultTable", resultTable);

        // 3.2 执行SQL进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, user_name from resultTable");

        // 3.3 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name, count(url) cnt from clickTable group by user_name");

        // 4. 创建一张用于输出的表
        tableEnv.executeSql("CREATE TABLE fileOutputTable (" +
                " url STRING" +
                ",user_name STRING" +
                ") WITH(" +
                " 'connector' = 'filesystem'" +
                ",'path' = 'output'" +
                ",'format' = 'csv'" +
                ")");

        // 创建一张用于控制台打印输出的表
        tableEnv.executeSql("CREATE TABLE printOutputTable(" +
                " user_name STRING" +
                ",cnt BIGINT" +
                ") WITH(" +
                " 'connector' = 'print'" +
                ")");

        // 5. table方式输出表
//        resultTable.executeInsert("fileOutputTable");
//        resultTable2.executeInsert("fileOutputTable");
//        aggResult.executeInsert("printOutputTable");

        // 6. sql方式输出表
//        tableEnv.executeSql("insert into fileOutputTable select url, user_name from resultTable");
        tableEnv.executeSql("insert into printOutputTable select user_name, count(url) cnt from clickTable group by user_name");
    }

}
