package com.chenpp.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation

/**
 * @author April.Chen
 * @date 2023/4/3 5:22 下午
 * */
object SparkHbaseDemo {

  def getHbaseConf(): Configuration = {
    val HBASE_CONFIG: Configuration = new Configuration()
    HBASE_CONFIG.set("hbase.zookeeper.quorum", "tdh57,tdh54,tdh50")
    HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH")
    HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH")
    HBASE_CONFIG.set("hbase.security.authentication", "kerberos")
    HBASE_CONFIG.set("zookeeper.znode.parent", "/hyperbase1")
    HBASE_CONFIG.set("hadoop.security.authentication", "kerberos")
    //    HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181")
    val conf = HBaseConfiguration.create(HBASE_CONFIG)
    conf
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/Users/chenpp/workspace/spark-demo/src/main/resources/jaas_hbase.conf")

    val conf: Configuration = getHbaseConf()
    //用户登录
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab("hbase/tdh50", "/Users/chenpp/workspace/spark-demo/src/main/resources/hyperbase.keytab")
    println("login end")
    createTable(conf)
  }

  //  def read(): Unit = {
  //    val conf: Configuration = HBaseConfiguration.create()
  //    conf.set("hbase.zookeeper.quorum", "tdh57,tdh54,tdh50")
  //    conf.set("hadoop.security.authentication", "Kerberos")
  //    conf.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH")
  //    conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH")
  //    //    conf.set("hbase.zookeeper.property.clientPort", "2181")
  //    conf.set("hbase.security.authentication", "kerberos")
  //
  //    System.setProperty("java.security.krb5.conf", "/Users/chenpp/workspace/spark-demo/src/main/resources/krb5.conf")
  //    //用户登录
  //    UserGroupInformation.setConfiguration(conf)
  //
  //    UserGroupInformation.loginUserFromKeytab("hbase", "/Users/chenpp/workspace/spark-demo/src/main/resources/hyperbase.keytab")
  //
  //    val sc = new SparkContext(new SparkConf())
  //    //设置查询的表名
  //    conf.set(TableInputFormat.INPUT_TABLE, "student")
  //    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
  //      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
  //      classOf[org.apache.hadoop.hbase.client.Result])
  //    val count = stuRDD.count()
  //    println("Students RDD Count:" + count)
  //    stuRDD.cache()
  //
  //    //遍历输出
  //    stuRDD.foreach({ case (_, result) =>
  //      val key = Bytes.toString(result.getRow)
  //      val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
  //      val gender = Bytes.toString(result.getValue("info".getBytes, "gender".getBytes))
  //      val age = Bytes.toString(result.getValue("info".getBytes, "age".getBytes))
  //      println("Row key:" + key + " Name:" + name + " Gender:" + gender + " Age:" + age)
  //    })
  //  }
  //
  //  def write(): Unit = {
  //    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkHbaseTablePuts")
  //    val sc = new SparkContext(conf)
  //    val rdd = sc.parallelize(20 until 30, 2)
  //    // 一个分区创建一个hbase连接，批量写入，效率高
  //    rdd.foreachPartition(it => {
  //      // 把每个Int 转成 Put对象
  //      val puts = it.map(f => {
  //        // 创建Put对象
  //        val put = new Put(Bytes.toBytes(s"spark_puts_${f}"))
  //        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(s"${f}"))
  //        put
  //      })
  //      val hbaseConf = HBaseConfiguration.create()
  //      var conn: Connection = null
  //      var table: HTable = null
  //      try {
  //        // 创建hbase连接
  //        conn = ConnectionFactory.createConnection(hbaseConf)
  //        // 创建表操作对象
  //        table = conn.getTable(TableName.valueOf("panniu:spark_user")).asInstanceOf[HTable]
  //        // 通过隐式转换，将scala的List转成javaList
  //        import scala.collection.convert.wrapAsJava.seqAsJavaList
  //        // 一个分区的数据批量写入
  //        table.put(puts.toList)
  //      } catch {
  //        case e: Exception => e.printStackTrace()
  //      } finally {
  //        table.close()
  //        conn.close()
  //      }
  //    })
  //  }
  //
  //  def login(): Unit = {
  //
  //  }


  def existsTable(tableName: String, conf: Configuration): Unit = {
    //    val  hbaseAdmin = new HBaseAdmin(conf)
    //    // 创建hbase连接
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //    val table = conn.getTable(TableName.valueOf(tableName))
    val admin: HBaseAdmin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    if (admin.tableExists(TableName.valueOf("default", tableName))) {
      print("table already exists!")
    } else {
      print("table not exists!")
    }
  }

  def createTable(conf: Configuration): Unit = {
    println("create hbase table")
    //    // 创建hbase连接
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: HBaseAdmin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    val columnDescBuilder: ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("family"))
      .setBlocksize(32 * 1024).setCompressionType(Compression.Algorithm.SNAPPY).setDataBlockEncoding(DataBlockEncoding.NONE)

    val table: TableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("default:test"))
      .setColumnFamily(columnDescBuilder.build()).build()
    admin.createTable(table)
    println("create hbase table end")

  }
}
