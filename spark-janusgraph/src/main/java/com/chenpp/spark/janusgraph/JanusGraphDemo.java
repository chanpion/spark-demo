package com.chenpp.spark.janusgraph;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

/**
 * @author April.Chen
 * @date 2023/6/13 7:11 下午
 **/
public class JanusGraphDemo {

    public static void main(String[] args) {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "tdh50,tdh54,tdh57")
                .set("storage.port", "2181")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "xx_lab")
                .set("storage.hbase.compat-class", "com.chenpp.spark.janusgraph.HBaseCompatKerberos")
                .set("storage.hbase.ext.zookeeper.znode.parent", "/hyperbase1");
        JanusGraph graph = builder.open();
        boolean open = graph.isOpen();
        System.out.println("is open:" + open);
    }
}
