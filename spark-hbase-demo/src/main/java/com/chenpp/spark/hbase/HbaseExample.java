package com.chenpp.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author April.Chen
 * @date 2023/4/10 9:26 下午
 **/
public class HbaseExample {

    private static final Configuration HBASE_CONFIG = HBaseConfiguration.create();

    public static void main(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);

//        System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf");

        loginArk();
//        creatTable("yuntu:demo", "c");
//
        addRecord("yuntu:demo", "222", "c", "a", "1");
        addRecord("yuntu:demo", "222", "c", "b", "2");

        scan("yuntu:demo");
    }

    public static void loginTdh() throws IOException {
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "tdh50:2181,tdh54:2181,tdh57:2181");
        HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.security.authentication", "kerberos");
        HBASE_CONFIG.set("zookeeper.znode.parent", "/hyperbase1");
        HBASE_CONFIG.set("hadoop.security.authentication", "kerberos");
        //    HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
        HBASE_CONFIG.set("keytab.file", "/Users/chenpp/tdh/hyperbase.keytab");
        HBASE_CONFIG.set("kerberos.principal", "hbase/tdh50");

        System.setProperty("java.security.krb5.conf", "/Users/chenpp/tdh/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/chenpp/workspace/spark-demo/src/main/resources/jaas_hbase.conf");
        LoginUtil.setJaasFile("hbase/tdh50", "/Users/chenpp/tdh/hyperbase.keytab");
        UserGroupInformation.setConfiguration(HBASE_CONFIG);
        UserGroupInformation.loginUserFromKeytab("hbase/tdh50", "/Users/chenpp/tdh/hyperbase.keytab");
        System.out.println("login success");
    }

    public static void loginArk() throws IOException {
        String principal = "hbase-cluster60@yuntu.com";
//        String principal = "hbase/yuntu-qiye-e-010058012060.hz.td@yuntu.com";
        String keytabPath = "/Users/chenpp/bigdata/60/ark3/hbase.headless.keytab";
        String krb5Pah = "/Users/chenpp/bigdata/60/ark3/krb5.conf";
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "yuntu-qiye-e-010058012060.hz.td,yuntu-qiye-e-010058012061.hz.td,yuntu-qiye-e-010058012062.hz.td");
//        HBASE_CONFIG.set("hbase.zookeeper.quorum", "10.58.12.60,10.58.12.61,10.58.12.62");
//        HBASE_CONFIG.set("hbase.zookeeper.quorum", "10.58.12.60");
        HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@yuntu.com");
        HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@yuntu.com");
        HBASE_CONFIG.set("hbase.security.authentication", "kerberos");
        HBASE_CONFIG.set("zookeeper.znode.parent", "/hbase-secure");
        HBASE_CONFIG.set("hadoop.security.authentication", "kerberos");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        HBASE_CONFIG.set("keytab.file", keytabPath);
        HBASE_CONFIG.set("kerberos.principal", principal);
        HBASE_CONFIG.set("hbase.client.retries.number", "3");
        HBASE_CONFIG.set("hbase.rpc.timeout", "30000");
        HBASE_CONFIG.set("hbase.client.opeation.timeout", "30000");

        System.setProperty("java.security.krb5.conf", krb5Pah);
//        System.setProperty("java.security.auth.login.config", "/Users/chenpp/bigdata/60/ark3/jaas.conf");
        LoginUtil.setJaasFile(principal, keytabPath);
        UserGroupInformation.setConfiguration(HBASE_CONFIG);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        System.out.println("login success");
    }

    public static void creatTable(String tableName, String familyName) throws Exception {
        System.out.println("create table");
        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
        //获取连接
        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            //拿到admin
            admin = conn.getAdmin();

            //获取表格描述器
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    //添加列簇
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName))
                    .build();
            //执行创建操作
            admin.createTable(tableDescriptor);
            //关流

            System.out.println("创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception {
        System.out.println("insert record");
        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);

            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void scan(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
        Connection connection = ConnectionFactory.createConnection(HBASE_CONFIG, User.create(UserGroupInformation.getLoginUser()));
        Table table = connection.getTable(TableName.valueOf(tableName));

        System.out.println("scan");
        Scan scan = new Scan();
        scan.setOneRowLimit();
        //todo workaround
        scan.setMaxResultSize(5 * 1024);

        ResultScanner rs = table.getScanner(scan);
        Set<String> res = new HashSet<>();
        for (Result r : rs) {
            List<String> qualifier = r.listCells().stream()
                    .map(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)))
                    .collect(Collectors.toList());
            res.addAll(qualifier);
        }
        System.out.println(res);

        HColumnDescriptor columnDescriptor = new HColumnDescriptor("");
        columnDescriptor.setBloomFilterType(BloomType.ROW);
    }

    public static void getRow(String tableOfName) throws java.lang.Exception {
        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
        Connection connection = ConnectionFactory.createConnection(HBASE_CONFIG, User.create(UserGroupInformation.getLoginUser()));

        TableName tableName = TableName.valueOf(tableOfName);
        //取得一个要操作的表
        Table table = connection.getTable(tableName);

        //设置要查询的行的rowkey
        Get wangwu = new Get(Bytes.toBytes("wangwu"));

        //设置显示多少个版本的数据
        wangwu.setMaxVersions(3);

        //取得指定时间戳的数据
        //wangwu.setTimeStamp(1);

        //限制要显示的列族
        wangwu.addFamily(Bytes.toBytes("grade"));
        //限制要显示的列
        //wangwu.addColumn(Bytes.toBytes("course"), Bytes.toBytes("yuwen"));
        Result result = table.get(wangwu);
        List<Cell> cells = result.listCells();
        for (Cell c : cells) {
            //注意这里 CellUtil类的使用
            System.out.print("行键:" + Bytes.toString(CellUtil.cloneRow(c)) + " ");
            System.out.print("列族:" + Bytes.toString(CellUtil.cloneFamily(c)) + " ");
            System.out.print("列:" + Bytes.toString(CellUtil.cloneQualifier(c)) + " ");
            System.out.print("值:" + Bytes.toString(CellUtil.cloneValue(c)) + " ");
            System.out.println();
        }

        //关闭资源
        table.close();
        //hbaseConn.close();
    }

    public static void getMultiRows(String tableOfName) throws java.lang.Exception {
        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
        Connection connection = ConnectionFactory.createConnection(HBASE_CONFIG, User.create(UserGroupInformation.getLoginUser()));

        TableName tableName = TableName.valueOf(tableOfName);
        //取得一个要操作的表
        Table table = connection.getTable(tableName);


        ArrayList<Get> getArrayList = new ArrayList<>();

        Get wangwu = new Get(Bytes.toBytes("wangwu"));
        //限制要显示的列族
        wangwu.addFamily(Bytes.toBytes("grade"));
        //限制要显示的列
        wangwu.addColumn(Bytes.toBytes("course"), Bytes.toBytes("yuwen"));

        Get lishi = new Get(Bytes.toBytes("lishi"));

        getArrayList.add(wangwu);
        getArrayList.add(lishi);

        Result[] results = table.get(getArrayList);
        for (int i = 0; i < results.length; i++) {
            Result result = results[i];
            List<Cell> cells = result.listCells();
            for (Cell c : cells) {
                //注意这里 CellUtil类的使用
                System.out.print("行键:" + Bytes.toString(CellUtil.cloneRow(c)) + " ");
                System.out.print("列族:" + Bytes.toString(CellUtil.cloneFamily(c)) + " ");
                System.out.print("列:" + Bytes.toString(CellUtil.cloneQualifier(c)) + " ");
                System.out.print("值:" + Bytes.toString(CellUtil.cloneValue(c)) + " ");
                System.out.println();
            }
        }
        //关闭资源
        table.close();
        //hbaseConn.close();
    }

    public static void showData(Result result) {
        while (result.advance()) {
            Cell cell = result.current();
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String val = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(row + "--->" + cf + "--->" + qualifier + "--->" + val);
        }
    }

}
