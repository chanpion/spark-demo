package com.chenpp.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
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

    static {
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "tdh50:2181,tdh54:2181,tdh57:2181");
        HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.security.authentication", "kerberos");
        HBASE_CONFIG.set("zookeeper.znode.parent", "/hyperbase1");
        HBASE_CONFIG.set("hadoop.security.authentication", "kerberos");
        //    HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
        HBASE_CONFIG.set("keytab.file" ,"/Users/chenpp/tdh/hyperbase.keytab");
        HBASE_CONFIG.set("kerberos.principal" , "hbase/tdh50");
    }

    public static void main(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);

//        System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf");
        System.setProperty("java.security.krb5.conf", "/Users/chenpp/tdh/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/chenpp/workspace/spark-demo/src/main/resources/jaas_hbase.conf");
        LoginUtil.setJaasFile("hbase/tdh50", "/Users/chenpp/tdh/hyperbase.keytab");
        UserGroupInformation.setConfiguration(HBASE_CONFIG);
        UserGroupInformation.loginUserFromKeytab("hbase/tdh50", "/Users/chenpp/tdh/hyperbase.keytab");
        System.out.println("login success");

//        creatTable("demo", "c");

//        addRecord("demo", "222", "c", "a", "1");
//        addRecord("demo", "222", "c", "b", "2");

        scan("demo");
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
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //添加列簇
            hTableDescriptor.addFamily(new HColumnDescriptor(familyName));
            //执行创建操作
            admin.createTable(hTableDescriptor);
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
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        System.out.println("scan");
        Scan scan = new Scan();
        scan.setOneRowLimit();
        //todo workaround
        scan.setMaxResultSize(5 * 1024);
//        scan.setFilter(new PageFilter(1));

        ResultScanner rs = table.getScanner(scan);

        Set<String> res = new HashSet<>();
        for (Result r : rs) {
            List<String> qualifier = r.listCells().stream()
                    .map(cell -> Bytes.toString(CellUtil.cloneQualifier(cell)))
                    .collect(Collectors.toList());
            res.addAll(qualifier);
        }
        System.out.println(res);
    }
}
