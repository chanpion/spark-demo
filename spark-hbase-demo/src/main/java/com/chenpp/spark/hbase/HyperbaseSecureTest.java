//package cn.tongdun.yuntu.module.demo;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.security.UserGroupInformation;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author April.Chen
// * @date 2023/4/4 11:47 上午
// **/
//public class HyperbaseSecureTest {
//    private static Configuration conf = null;
//
//    static {
//        Configuration HBASE_CONFIG = new Configuration();
//        HBASE_CONFIG.set("hbase.zookeeper.quorum", "tdh50,tdh54, tdh57");
//        HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH");
//        HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH");
//        HBASE_CONFIG.set("hbase.security.authentication", "kerberos");
//        HBASE_CONFIG.set("zookeeper.znode.parent", "/hyperbase1");
//        HBASE_CONFIG.set("hadoop.security.authentication", "kerberos");
//
//        conf = HBaseConfiguration.create(HBASE_CONFIG);
//    }
//
//    /**
//     * 创建一张表
//     */
//    public static void creatTable(String tableName, String[] familys) throws Exception {
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        if (admin.tableExists(tableName)) {
//            System.out.println("table already exists!");
//        } else {
//            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
//            for(int i=0; i<familys.length; i++){
//                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
//            }
//            admin.createTable(tableDesc);
//            System.out.println("create table " + tableName + " ok.");
//        }
//    }
//
//    /**
//     * 删除表
//     */
//    public static void deleteTable(String tableName) throws Exception {
//        try {
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//            System.out.println("delete table " + tableName + " ok.");
//        } catch (MasterNotRunningException e) {
//            e.printStackTrace();
//        } catch (ZooKeeperConnectionException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 插入一行记录
//     */
//    public static void addRecord (String tableName, String rowKey, String family, String qualifier, String value)
//            throws Exception{
//        try {
//            HTable table = new HTable(conf, tableName);
//            Put put = new Put(Bytes.toBytes(rowKey));
//            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
//            table.put(put);
//            System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 删除一行记录
//     */
//    public static void delRecord (String tableName, String rowKey) throws IOException{
//        HTable table = new HTable(conf, tableName);
//        List list = new ArrayList();
//        Delete del = new Delete(rowKey.getBytes());
//        list.add(del);
//        table.delete(list);
//        System.out.println("del recored " + rowKey + " ok.");
//    }
//
//    /**
//     * 查找一行记录
//     */
//    public static void getOneRecord (String tableName, String rowKey) throws IOException{
//        HTable table = new HTable(conf, tableName);
//        Get get = new Get(rowKey.getBytes());
//        Result rs = table.get(get);
//        for(KeyValue kv : rs.raw()){
//            System.out.print(new String(kv.getRow()) + " " );
//            System.out.print(new String(kv.getFamily()) + ":" );
//            System.out.print(new String(kv.getQualifier()) + " " );
//            System.out.print(kv.getTimestamp() + " " );
//            System.out.println(new String(kv.getValue()));
//        }
//    }
//
//    /**
//     * 显示所有数据
//     */
//    public static void getAllRecord (String tableName) {
//        try{
//            HTable table = new HTable(conf, tableName);
//            Scan s = new Scan();
//            ResultScanner ss = table.getScanner(s);
//            for(Result r:ss){
//                for(KeyValue kv : r.raw()){
//                    System.out.print(new String(kv.getRow()) + " ");
//                    System.out.print(new String(kv.getFamily()) + ":");
//                    System.out.print(new String(kv.getQualifier()) + " ");
//                    System.out.print(kv.getTimestamp() + " ");
//                    System.out.println(new String(kv.getValue()));
//                }
//            }
//        } catch (IOException e){
//            e.printStackTrace();
//        }
//    }
//
//    public static void main(String[] args) throws IOException {
//        try {
//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab("hbase/transwarp-node5", "/etc/hyperbase1/hbase.keytab");
////        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
////        hBaseAdmin.createTable(new HTableDescriptor("gordon2"));
//
//            String tablename = "scores";
//            String[] familys = {"grade", "course"};
//            HyperbaseSecureTest.creatTable(tablename, familys);
//
//            //add record zkb
//            HyperbaseSecureTest.addRecord(tablename,"zkb","grade","","5");
//            HyperbaseSecureTest.addRecord(tablename,"zkb","course","","90");
//            HyperbaseSecureTest.addRecord(tablename,"zkb","course","math","97");
//            HyperbaseSecureTest.addRecord(tablename,"zkb","course","art","87");
//            //add record  baoniu
//            HyperbaseSecureTest.addRecord(tablename,"baoniu","grade","","4");
//            HyperbaseSecureTest.addRecord(tablename,"baoniu","course","math","89");
//
//            System.out.println("===========get one record========");
//            HyperbaseSecureTest.getOneRecord(tablename, "zkb");
//
//            System.out.println("===========show all record========");
//            HyperbaseSecureTest.getAllRecord(tablename);
//
//            System.out.println("===========del one record========");
//            HyperbaseSecureTest.delRecord(tablename, "baoniu");
//            HyperbaseSecureTest.getAllRecord(tablename);
//
//            System.out.println("===========show all record========");
//            HyperbaseSecureTest.getAllRecord(tablename);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
