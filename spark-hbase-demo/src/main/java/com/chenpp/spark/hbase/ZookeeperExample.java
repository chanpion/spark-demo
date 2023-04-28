package com.chenpp.spark.hbase;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author April.Chen
 * @date 2023/4/4 4:33 下午
 **/
public class ZookeeperExample {


    private static String connectString = "tdh50:2181";
    private static int sessionTimeout = 200 * 1000;

    static {
        System.setProperty("java.security.auth.login.config", "/Users/chenpp/workspace/spark-demo/src/main/resources/jaas.conf");
        System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf");
        System.setProperty("zookeeper.server.principal", "zookeeper/tdh50@TDH");
    }

    public static void main(String[] args) {
        try {
            ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
                System.out.println("watch");
            });

            System.out.println(zooKeeper.getState());
            Stat stat = new Stat();

            byte[] data = zooKeeper.getData("/hyperbase1", false, stat);
            System.out.println(new String(data));
            System.out.println(stat);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
