package com.chenpp.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation

/**
 * @author April.Chen
 * @date 2023/4/4 3:11 下午
 * */
object SparkHyperbase {

  def main(args: Array[String]): Unit = {
    System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/Users/chenpp/workspace/spark-demo/src/main/resources/jaas_hbase.conf")
    UserGroupInformation.loginUserFromKeytab("hbase/tdh50", "/Users/chenpp/workspace/spark-demo/src/main/resources/hyperbase.keytab")
    println("login end")
    val conf: Configuration = HBaseConfiguration.create()
    conf.addResource(new Path("/Users/chenpp/workspace/spark-demo/src/main/resources/conf/core-site.xml"))
    conf.addResource(new Path("/Users/chenpp/workspace/spark-demo/src/main/resources/conf/hbase-site.xml"))
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: HBaseAdmin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    println("create hbase table")

    val columnDescBuilder: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("family"))
      .setBlocksize(32 * 1024).setCompressionType(Compression.Algorithm.SNAPPY).setDataBlockEncoding(DataBlockEncoding.NONE)

    val table: TableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("default:test"))
      .setColumnFamily(columnDescBuilder.build()).build()
    admin.createTable(table)
    println("create hbase table end")

  }
}
