package com.lysoft.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 功能说明：自定义sink算子，将数据写入hbase。
 * author:liangyy
 * createtime：2022-12-28 21:50:10
 */
public class Sink2HbaseCustom {

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

        //3. 构建hbase sink
        HbaseSinkFunction hbaseSinkFunction = new HbaseSinkFunction();

        //4. 写入hbase
        dataStreamSource.addSink(hbaseSinkFunction);

        env.execute();
    }

    public static class HbaseSinkFunction extends RichSinkFunction<Event> {
        public Connection connection;

        public org.apache.hadoop.conf.Configuration configuration;

        @Override
        public void open(Configuration parameters) throws Exception {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "localhost:2181");
            connection = ConnectionFactory.createConnection(configuration);
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            Table table = connection.getTable(TableName.valueOf("clicks")); //表名为clicks
            Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8)); //指定rowkey

            put.addColumn("info".getBytes(StandardCharsets.UTF_8) //指定列名
                    , value.toString().getBytes(StandardCharsets.UTF_8) //写入的数据
                    , "1".getBytes(StandardCharsets.UTF_8)); //写入的数据
            table.put(put); //执行put操作
            table.close(); //将表关闭
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

    }

}
