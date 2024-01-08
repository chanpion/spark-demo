package com.chenpp.spark.hdfs;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class HdfsFileInfo implements Serializable {

    private static final long serialVersionUID = -7363845568665570807L;
    private String name;
    private String type;
    private String owner;
    private String group;
    private String auth;
    private String displayFileSize;
    private long fileSize;
    private Date modifyTime;
    private String path;
    private String suffix;

    public boolean isFile() {
        return "FILE".equalsIgnoreCase(type);
    }

    public boolean isDirectory() {
        return "DIR".equalsIgnoreCase(type);
    }
}
