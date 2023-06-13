package com.chenpp.spark.janusgraph;

import cn.tongdun.yuntu.haina.client.util.YuntuKerberosUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.security.User;
import org.janusgraph.diskstorage.hbase.ConnectionMask;
import org.janusgraph.diskstorage.hbase.HBaseCompat;
import org.janusgraph.diskstorage.hbase.HConnection1_0;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;

import java.io.IOException;

/**
 * @author April.Chen
 * @date 2023/6/13 7:25 下午
 **/
public class HBaseCompatKerberos implements HBaseCompat {
    @Override
    public void setCompression(HColumnDescriptor cd, String algorithm) {
        cd.setCompressionType(Compression.Algorithm.valueOf(algorithm));
    }

    @Override
    public HTableDescriptor newTableDescriptor(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        return new HTableDescriptor(tn);
    }

    @Override
    public ConnectionMask createConnection(Configuration conf) throws IOException {
        String krb5Path = "/Users/chenpp/tdh/krb5.conf";
        String keytabPath = "/Users/chenpp/tdh/hyperbase.keytab";
        String principal = "hbase/tdh50";

        conf.set("hbase.zookeeper.quorum", "tdh50:2181,tdh54:2181,tdh57:2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hyperbase1");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("keytab.file", "/Users/chenpp/tdh/hyperbase.keytab");
        conf.set("kerberos.principal", "hbase/tdh50");
        conf.set("cluster.max-partitions", "12");
        conf.set(SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS.toStringWithoutRoot(), "12");
        User user = YuntuKerberosUtil.getAuthenticatedUser(krb5Path, keytabPath, principal, "kerberos", false);
        Connection conn = ConnectionFactory.createConnection(conf, user);
        return new HConnection1_0(conn);
    }

    @Override
    public void addColumnFamilyToTableDescriptor(HTableDescriptor tableDescriptor, HColumnDescriptor columnDescriptor) {
        tableDescriptor.addFamily(columnDescriptor);
    }

    @Override
    public void setTimestamp(Delete d, long timestamp) {
        d.setTimestamp(timestamp);
    }
}
