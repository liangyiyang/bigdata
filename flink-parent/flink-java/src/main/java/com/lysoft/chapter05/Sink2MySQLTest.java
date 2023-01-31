package com.lysoft.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能说明：测试sink算子，将数据写入MySQL。
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class Sink2MySQLTest {

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 构建数据
        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        //3. 构建mysql sink
        SinkFunction<Event> mysqlSink = JdbcSink.sink("insert into clicks(user, url) values(?, ?)", new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement ps, Event event) throws SQLException {
                        ps.setString(1, event.getUser());
                        ps.setString(2, event.getUrl());
                    }
                }
                , JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(5)
                        .build()
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        //4. 写入mysql
        dataStreamSource.addSink(mysqlSink);

        env.execute();
    }

}
