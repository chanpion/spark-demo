package com.chenpp.spark.hdfs;

import lombok.Data;

import java.io.Serializable;


/**
 * @author admin
 */
@Data
public class HdfsArg implements Serializable {

    private static final long serialVersionUID = 996914174468812330L;
    private String namespace;
    private String ns1;
    private String ns2;
    private String user;

}
