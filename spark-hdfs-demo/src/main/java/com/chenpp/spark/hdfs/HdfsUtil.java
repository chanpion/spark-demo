package com.chenpp.spark.hdfs;

import com.google.common.base.Splitter;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author April.Chen
 * @date 2023/8/28 10:43 上午
 **/
public class HdfsUtil {

    private static Logger logger = LoggerFactory.getLogger(HdfsUtil.class);

    public static final String HDFS_PREFIX = "hdfs://";
    private final static long KB = 1024L;
    public final static long MB = KB * KB;
    public final static long GB = KB * MB;
    private final static long TB = KB * GB;

    private static FileSystem getInstance(HdfsArg hdfsArg) {
        Configuration configuration = new Configuration();
        String nameService = hdfsArg.getNamespace();
        String ns1 = hdfsArg.getNs1();
        String ns2 = hdfsArg.getNs2();
        String user = hdfsArg.getUser();
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
        configuration.set("dfs.client.failover.proxy.provider." + nameService,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        try {
            return FileSystem.get(new URI(hdfsUrl), configuration, user);
        } catch (Exception e) {
            throw new RuntimeException("failed to connect hdfs", e);
        }
    }


    /**
     * 检验hdfs是否连接成功
     *
     * @return
     */
    public static boolean check(HdfsArg hdfsArg, String userdir) {
        try {
            if (StringUtils.isBlank(userdir)) {
                throw new RuntimeException("请配置HDFS目录");
            }
            return existFile(hdfsArg, userdir, null);
        } catch (Exception e) {
            throw new RuntimeException("failed to connect hdfs", e);
        }
    }

    /**
     * 根据路径列出文件
     *
     * @param path
     * @return
     */
    public static List<HdfsFileInfo> listFiles(HdfsArg hdfsArg, String path) {
        try {
            FileStatus[] fileStatus = getInstance(hdfsArg).listStatus(new Path(path));
            List<HdfsFileInfo> files = new ArrayList();
            if (fileStatus == null || fileStatus.length == 0) {
                return files;
            }
            files = Arrays.stream(fileStatus)
                    .map(one -> transFileStatus(one))
                    .sorted(Comparator.comparing(HdfsFileInfo::getModifyTime).reversed())
                    .collect(Collectors.toList());
            return files;
        } catch (IOException e) {
            throw new RuntimeException("不存在此目录或文件:" + path, e);
        }
    }

    /**
     * 转换文件dto
     *
     * @param file 文件
     * @return
     */
    private static HdfsFileInfo transFileStatus(FileStatus file) {
        HdfsFileInfo hdfsFileInfo = new HdfsFileInfo();
        Path filePath = file.getPath();
        String name = filePath.getName();

        int i = name.lastIndexOf(".");
        if (i >= 0 && i < name.length() - 1) {
            hdfsFileInfo.setSuffix(name.substring(i + 1));
        } else {
            hdfsFileInfo.setSuffix("");
        }
        hdfsFileInfo.setName(name);
        hdfsFileInfo.setPath(filePath.toString());
        hdfsFileInfo.setAuth(file.getPermission().toString());
        hdfsFileInfo.setOwner(file.getOwner());
        hdfsFileInfo.setGroup(file.getGroup());
        hdfsFileInfo.setType(file.isDirectory() ? "DIR" : "FILE");
        hdfsFileInfo.setModifyTime(new Date(file.getModificationTime()));
        hdfsFileInfo.setFileSize(file.getLen());
        if (hdfsFileInfo.isDirectory()) {
            hdfsFileInfo.setDisplayFileSize("");
        } else {
            hdfsFileInfo.setDisplayFileSize(getFileSize(file.getLen()));
        }
        return hdfsFileInfo;
    }

    /**
     * 获取文件大小
     *
     * @param m
     * @return
     */
    private static String getFileSize(long m) {
        if (m > TB) {
            return getPercent(m, TB) + "TB";
        } else if (m > GB) {
            return getPercent(m, GB) + "GB";
        } else if (m > MB) {
            return getPercent(m, MB) + "MB";
        } else if (m > KB) {
            return getPercent(m, KB) + "KB";
        }
        return m + "B";
    }

    /**
     * 获取百分比
     *
     * @param x
     * @param k
     * @return
     */
    private static String getPercent(long x, long k) {
        int last = Math.round((x % k) * 100 / (float) k);
        if (last == 0) {
            return x / k + "";
        }
        return x / k + "." + last;
    }

    /**
     * 上传文件
     *
     * @param path
     * @param fileName
     * @param is
     * @return
     */
    public static String uploadFile(HdfsArg hdfsArg, String path, String fileName, InputStream is) {
        try {
            FileSystem fs = getInstance(hdfsArg);
            Path dst = getFilePath(path, fileName);
            FSDataOutputStream output = fs.create(dst);
            byte[] bytes = new byte[1024 * 1024 * 10];
            int len = 0;
            while ((len = is.read(bytes)) != -1) {
                output.write(bytes, 0, len);
            }
            output.flush();
            output.close();
            is.close();
            String finalFileName = dst.toString();
            if (!finalFileName.startsWith(HDFS_PREFIX)) {
                finalFileName = String.format("%s%s", fs.getUri(), dst.toString());
            }
            return finalFileName;
        } catch (Exception e) {
            logger.error("上传文件异常", e);
            throw new RuntimeException("上传文件异常." + e.getMessage(), e);
        }
    }

    /**
     * 获取文件路径
     *
     * @param path     路径
     * @param fileName 文件
     * @return 文件路径
     */
    public static Path getFilePath(String path, String fileName) {
        if (fileName == null) {
            return new Path(path);
        }
        if (!path.endsWith(File.separator)) {
            path = path + File.separator;
        }
        if (fileName.startsWith(File.separator) && fileName.length() > 1) {
            fileName = fileName.substring(1);
        }
        return new Path(path + fileName);
    }

    /**
     * 下载文件
     *
     * @param path
     * @param fileName
     * @return
     */
    public static String download(HdfsArg hdfsArg, String path, String fileName) {
        try {
            FileSystem fs = getInstance(hdfsArg);
            String root = "/tmp/download_qiming/" + RandomDataUtil.getRandomStr(8);
            new File(root).mkdirs();
            Path dst = getFilePath(root, fileName);
            //判断文件大小
            HdfsFileInfo fileInfo = getFileInfo(hdfsArg, path, fileName);
            if (fileInfo.isDirectory()) {
                throw new RuntimeException("不支持下载目录");
            }
            if (fileInfo.getFileSize() > GB) {
                throw new RuntimeException("文件过大，暂时不支持下载");
            }
            fs.copyToLocalFile(getFilePath(path, fileName), dst);
            return dst.toString();
        } catch (Exception e) {
            logger.error("下载文件异常", e);
            throw new RuntimeException("下载文件异常." + e.getMessage(), e);
        }
    }

    /**
     * 获取文件信息
     *
     * @param path
     * @param fileName
     * @return
     */
    public static HdfsFileInfo getFileInfo(HdfsArg hdfsArg, String path, String fileName) {
        try {
            FileStatus fileStatus = getInstance(hdfsArg).getFileStatus(getFilePath(path, fileName));
            return transFileStatus(fileStatus);
        } catch (Exception e) {
            logger.error("获取文件信息异常", e);
            throw new RuntimeException("获取文件信息异常." + e.getMessage(), e);
        }
    }


    /**
     * 创建目录
     *
     * @param path
     * @return
     */
    public static boolean mkdir(HdfsArg hdfsArg, String path) {
        try {
            return getInstance(hdfsArg).mkdirs(getFilePath(path, null));
        } catch (RemoteException ex) {
            if (ex.getClassName().contains("PathComponentTooLongException")) {
                throw new RuntimeException("HDFS_PATH_NAME_TOO_LONG", ex);
            }
            throw new RuntimeException(ex.getMessage(), ex);
        } catch (Exception e) {
            logger.error("创建目录异常", e);
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param path
     * @param fileName
     * @return
     */
    public static boolean existFile(HdfsArg hdfsArg, String path, String fileName) {

        try {
            return getInstance(hdfsArg).exists(getFilePath(path, fileName));
        } catch (Exception e) {
            logger.error("判断文件是否存在异常", e);
            throw new RuntimeException("判断文件是否存在异常." + e.getMessage(), e);
        }
    }


    /**
     * 删除文件
     *
     * @param path
     * @param fileName
     * @return
     */
    public static boolean deleteFile(HdfsArg hdfsArg, String path, String fileName) {
        try {
            return getInstance(hdfsArg).delete(getFilePath(path, fileName), true);
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * byte to string
     *
     * @param bs
     * @param len
     * @return
     */
    private static String byte2str(byte[] bs, int len) {
        byte[] target = Arrays.copyOf(bs, len);
        return new String(target, StandardCharsets.UTF_8);
    }

    /**
     * 读取文件部分内容
     *
     * @param path
     * @param limitLines
     * @return
     */
    public static List<String> readFileHeadContent(HdfsArg hdfsArg, String path, int limitLines) {
        try {
            FileSystem fs = getInstance(hdfsArg);

            //判断文件类型
            FileStatus fileStatus = fs.getFileStatus(getFilePath(path, null));
            boolean isDir = fileStatus.isDirectory();
            if (isDir) {
                List<HdfsFileInfo> cfiles = listFiles(hdfsArg, path);
                if (cfiles == null) {
                    throw new RuntimeException("预览目录失败");
                }
                String filePath = null;
                for (int i = 0; i < cfiles.size(); i++) {
                    HdfsFileInfo file = cfiles.get(i);
                    if (file.getFileSize() > 1 && StringUtils.startsWithIgnoreCase(file.getName(), "part-")) {
                        filePath = file.getPath();
                        break;
                    }
                }
                if (filePath == null) {
                    throw new RuntimeException("预览目录失败, 请确认该目录下包含part开头的分区文件");
                }
                path = filePath;
            }
            FSDataInputStream in = fs.open(getFilePath(path, null));

            BOMInputStream fsdis = new BOMInputStream(in, ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
                    ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE
            );

            //最多读取100K * 100次（大约是100M)
            //如果是csv文件， 读了11行就返回， 如果是json, 读取100行就返回
            int times = 0;
            int onceLen = 1024 * 100;
            int maxTimes = 100;
            byte[] bs = new byte[onceLen];
            int len = fsdis.read(bs);
            StringBuilder sb = new StringBuilder();
            String last = "";
            boolean lastEnd = true;
            int lines = 0;
            while (len != -1 && times < maxTimes) {
                String m = byte2str(bs, len);
                sb.append(m);
                //判断读取行数
                lines += DataUtil.getBreakLines(m);
                if (lines > limitLines) {
                    break;
                }
                len = fsdis.read(bs);
                times++;
            }
            fsdis.close();
            String resultStr = sb.toString();
            List<String> all = Splitter.on("\n").splitToList(resultStr);
            if (times < maxTimes) {
                return all;
            }
            return all.subList(0, all.size() - 1);
        } catch (Exception e) {
             throw new RuntimeException("读取异常", e);
        }
    }

    /**
     * 重命名文件
     *
     * @param path
     * @param fileName
     * @param newFileName
     * @return
     */
    public static boolean renameFile(HdfsArg hdfsArg, String path, String fileName, String newFileName) {
        if (StringUtils.equalsIgnoreCase(fileName, newFileName)) {
            throw new RuntimeException("新名称不能和旧名称一样");
        }
        if (existFile(hdfsArg, path, newFileName)) {
            throw new RuntimeException("新名称已存在");
        }
        try {
            boolean result = getInstance(hdfsArg).rename(getFilePath(path, fileName), getFilePath(path, newFileName));
            if (!result) {
                throw new RuntimeException("重命名失败");
            }
            return true;
        } catch (RemoteException ex) {
            if (ex.getClassName().contains("PathComponentTooLongException")) {
                throw new RuntimeException("HDFS_PATH_NAME_TOO_LONG", ex);
            }
            throw new RuntimeException(ex.getMessage(), ex);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 移动文件
     *
     * @param path
     * @param fileName
     * @param newPath
     * @return
     */
    public static boolean moveFile(HdfsArg hdfsArg, String path, String fileName, String newPath) {
        if (StringUtils.equalsIgnoreCase(path, newPath)) {
            throw new RuntimeException("新目录不能和旧目录一样");
        }
        try {
            if (!mkdir(hdfsArg, newPath)) {
                throw new RuntimeException("新目录创建失败");
            }
            boolean result = getInstance(hdfsArg).rename(getFilePath(path, fileName), getFilePath(newPath, fileName));
            if (!result) {
                throw new RuntimeException("移动失败");
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * 从一个hdfs集群复制文件到另一个hdfs集群文件
     *
     * @param srcHdfsArg 源HDFS配置
     * @param srcPath    源文件路径
     * @param fileName   文件名
     * @param desHdfsArg 目的HDFS配置
     * @param desPath    目的文件路径
     * @return
     */
    public static String discp(HdfsArg srcHdfsArg, String srcPath, String fileName, HdfsArg desHdfsArg, String desPath) {
        try {
            if (!existFile(srcHdfsArg, srcPath, fileName)) {
                throw new RuntimeException("源文件不存在");
            }
            String tmpFile = download(srcHdfsArg, srcPath, fileName);
            File file = new File(tmpFile);
            InputStream is = new FileInputStream(file);
            String path = uploadFile(desHdfsArg, desPath, fileName, is);
            boolean delRst = file.delete();
            logger.info("删除文件结果: " + delRst);
            return path;
        } catch (Exception e) {
            logger.error("导入文件异常", e);
            throw new RuntimeException("导入文件异常." + e.getMessage(), e);
        }
    }
}
