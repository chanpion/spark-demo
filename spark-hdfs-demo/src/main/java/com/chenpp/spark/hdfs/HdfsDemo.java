package com.chenpp.spark.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.URI;

/**
 * @author April.Chen
 * @date 2023/5/15 11:20 上午
 **/
public class HdfsDemo {
    public static void main(String[] args) throws Exception {
        //1、连接的集群地址
        //访问nameNode的端口，这里的 h01系统并不知道是多少，所以，需要在本地的host中增加映射
        //端口就是 core-site.xml中的nameNode地址，并不都是8020
        URI uri = new URI("hdfs://h01:8020");
        //2、创建配置文件
        Configuration config = new Configuration();
        //指定用户，为什么要指定用户，因为hdfs中，文件是有所属权限的，有Owner、Group
        String user = "root";
        //3、获取到了客户端对象
        FileSystem fileSystem = FileSystem.get(uri, config, user);
        //创建hdfs文件夹
        fileSystem.mkdirs(new Path("/Beijing"));
        //4、关闭连接
        fileSystem.close();
        UserGroupInformation.reset();
    }
}
