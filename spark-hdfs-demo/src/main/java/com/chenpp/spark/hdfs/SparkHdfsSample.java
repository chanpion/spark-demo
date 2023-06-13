package com.chenpp.spark.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URI;

/**
 * @author April.Chen
 * @date 2023/4/6 2:49 下午
 **/
public class SparkHdfsSample {
    private static FileSystem getInstance() throws Exception {
        Configuration configuration = new Configuration();
        String nameService = "ns60";
        String ns1 = "yuntu-d-010058012060.hz.td:8020";
        String ns2 = "yuntu-d-010058012061.hz.td:8020";
        String user = null;
        String hdfsUrl = "hdfs://" + nameService;
        if (nameService.startsWith("hdfs://")) {
            hdfsUrl = nameService;
        }

        configuration.set("fs.defaultFS", hdfsUrl);
        configuration.set("dfs.nameservices", nameService);
        configuration.set("dfs.ha.namenodes." + nameService, "nn1,nn2");
        configuration.set("dfs.namenode.rpc-address." + nameService + ".nn1", ns1);
        configuration.set("dfs.namenode.rpc-address." + nameService + ".nn2", ns2);
        configuration.set("fs.hdfs.impl.disable.cache", "true");
        configuration.set("dfs.client.failover.proxy.provider." + nameService, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        return FileSystem.get(new URI(hdfsUrl), configuration, user);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/spark-hdfs-demo/src/main/resources/krb5.conf");

        Configuration configuration = new Configuration(false);
        configuration.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(configuration);
        //用户登录
        UserGroupInformation.loginUserFromKeytab("admin/admin@yuntu.com", "/Users/chenpp/workspace/spark-demo/spark-hdfs-demo/src/main/resources/admin.keytab");
        System.out.println("login end");
        FileSystem fileSystem = getInstance();
        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/cpp"));
        for (FileStatus status : fileStatus) {
            System.out.println(status);
        }

        readFromHdfs();
    }

    public static void readFromHdfs() {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Hdfs Demo")
                .master("yarn-client")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("/cpp/data/客户风险特征.csv");
        df.show(10);
        spark.close();
    }
}
