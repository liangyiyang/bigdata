package com.lysoft.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestAPI {

    private Connection connection = null;
    private Admin admin = null;

    /**
     * 初始化连接
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    /**
     * 清理连接
     *
     * @throws IOException
     */
    @After
    public void clear() throws IOException {
        if (admin != null) {
            admin.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 判断表是否存在
     *
     * @throws IOException
     */
    @Test
    public void testTableExists() throws IOException {
        String tableName = "student";
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        System.out.println(exists ? "表[" + tableName + "]已经存在!" : "表[" + tableName + "]不存在!");
    }

    /**
     * 创建表和列族
     *
     * @throws IOException
     */
    @Test
    public void testCreateTable() throws IOException {
        String tableName = "stu1";
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表[" + tableName + "]已经存在!");
            return;
        }

        //添加多个列族
        String[] cfs = {"info1", "info2"};

        //创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            //创建列族描述器
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);

        System.out.println("表[" + tableName + "]创建成功!");
    }

    /**
     * 删除表
     *
     * @throws IOException
     */
    @Test
    public void testDropTable() throws IOException {
        String tableName = "stu1";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表[" + tableName + "]不存在!");
            return;
        }

        //下线表
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("表[" + tableName + "]删除成功!");
    }

    /**
     * 插入数据
     *
     * @throws IOException
     */
    @Test
    public void testPutData() throws IOException {
        String tableName = "stu1";
        Table table = connection.getTable(TableName.valueOf(tableName));
        String rowkey = "1001";

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"), Bytes.toBytes("张三"));

        table.put(put);
        table.close();
        System.out.println("插入数据成功!");
    }

    /**
     * 根据rowKey获取数据
     *
     * @throws IOException
     */
    @Test
    public void testGetData() throws IOException {
        String tableName = "stu1";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes("1001"));
        //获取某个列族的列
        //get.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("name"));

        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    "CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            ", Value:" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
    }

    /**
     * 扫描表数据
     *
     * @throws IOException
     */
    @Test
    public void testScanTable() throws IOException {
        String tableName = "stu1";
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                                ", CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                ", CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                ", Value:" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        table.close();
    }

    /**
     * 删除列族或列
     *
     * @throws IOException
     */
    @Test
    public void testDeleteData() throws IOException {
        String tableName = "stu1";
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes("1002"));

        //删除指定列

        //当创建表表时指定了列族存储多个版本数据时，这种删除只会删除最新版本的数据，生产慎用
        //delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"));

        //当创建表表时指定了列族存储多个版本数据时，这种删除会删除所有版本的数据
        //delete.addColumns(Bytes.toBytes("info1"), Bytes.toBytes("age"));

        //删除整个列族，在hbase shell中是无法删除整个列族，java API中可以删除
        delete.addFamily(Bytes.toBytes("info2"));
        table.delete(delete);
        table.close();
    }


    /**
     * 添加命名空间
     * @throws IOException
     */
    @Test
    public void testCreateNameSpace() throws IOException {
        String namespace = "gmall";
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(namespace + "命名空间已存在!");
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(namespace + "命名空间已创建!");
    }

}
