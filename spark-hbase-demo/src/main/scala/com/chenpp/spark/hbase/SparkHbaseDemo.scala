package com.chenpp.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.{BulkLoadHFiles, BulkLoadHFilesTool, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

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

  def bulkLoad(df: DataFrame, conf: Configuration, tableName: String, familyName: String, hFilePath: String)(implicit spark: SparkSession): Unit = {
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin

    /**
     * 保存生成的HFile文件
     * 注：bulk load  生成的HFile文件需要落地
     * 然后再通过LoadIncrementalHFiles类load进Hbase
     * 此处关于  sortBy 操作详解：
     * 0. Hbase查询是根据rowkey进行查询的，并且rowkey是有序，
     * 某种程度上来说rowkey就是一个索引，这是Hbase查询高效的一个原因，
     * 这就要求我们在插入数据的时候，要插在rowkey该在的位置。
     * 1. Put方式插入数据，会有WAL，同时在插入Hbase的时候会根据RowKey的值选择合适的位置，此方式本身就可以保证RowKey有序
     * 2. bulk load 方式没有WAL，它更像是hive通过load方式直接将底层文件HFile移动到制定的Hbase路径下，所以，在不东HFile的情况下，要保证本身有序才行
     * 之前写的时候只要rowkey有序即可，但是2.0.2版本的时候发现clounm也要有序，所以会有sortBy(x => (x._1, x._2.getKeyString), true)
     *
     * @param hfileRDD
     */
    // 0. 准备程序运行的环境
    // 如果 HBase 表不存在，就创建一个新表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val hcd = new HColumnDescriptor(familyName)
      desc.addFamily(hcd)
      admin.createTable(desc)
      print("创建了一个新表")
    }
    val fileSystem = FileSystem.get(conf)
    // 如果存放 HFile文件的路径已经存在，就删除掉
    if (fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
      print("删除hdfs上存在的路径")
    }

    // 1. 清洗需要存放到 HFile 中的数据，rowKey 一定要排序，否则会报错：
    // java.io.IOException: Added a key not lexically larger than previous.
    val arr = Array("code", "description", "total_emp", "salary") //列的名字集合
    val data: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = spark.sparkContext.textFile("dataSourcePath")
      .map(row => {
        // 处理数据的逻辑
        val arrs = row.split("\t")
        var kvlist: Seq[KeyValue] = List() //存储多个列
        var rowkey: Array[Byte] = null
        var cn: Array[Byte] = null
        var v: Array[Byte] = null
        var kv: KeyValue = null
        val cf = familyName.getBytes //列族
        rowkey = Bytes.toBytes(arrs(0)) //key
        for (i <- 1 to (arrs.length - 1)) {
          cn = arr(i).getBytes() //列的名称
          v = Bytes.toBytes(arrs(i)) //列的值
          //将rdd转换成HFile需要的格式,上面定义了Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
          kv = new KeyValue(rowkey, cf, cn, v) //封装一下 rowkey, cf, clounmVale, value
          kvlist = kvlist :+ kv //将新的kv加在kvlist后面（不能反 需要整体有序）
        }
        (new ImmutableBytesWritable(rowkey), kvlist)
      })

    val hfileRDD: RDD[(ImmutableBytesWritable, KeyValue)] = data
      .flatMapValues(_.iterator)


    // 2. Save Hfiles on HDFS
    val table = conn.getTable(TableName.valueOf(tableName))
    val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor)

    //    HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor, regionLocator)

    hfileRDD
      .sortBy(x => (x._1, x._2.getKeyString), true) //要保持 整体有序
      .saveAsNewAPIHadoopFile(hFilePath,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        conf)
    print("成功生成HFILE")


    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)
    conn.close()
  }

  def transDataFrameToRDDHFile(df: DataFrame, rowKeyField: String = "uid", cf: String = "c"): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val data = df.rdd
      .flatMap(row => {
        val rowKey = row.getAs(rowKeyField).toString
        val indicator_code = row.getAs[String]("indicator_code")
        val indicator_value = row.getAs[String]("indicator_value")
        val uid_code = rowKeyField
        val uid_value = row.getAs[String]("uid")
        Seq((rowKey, indicator_code, indicator_value), (rowKey, uid_code, uid_value))
      })
      .distinct()
      .sortBy(x => (x._1, x._2), ascending = true)
      .map(e => {
        val kv = new KeyValue(Bytes.toBytes(e._1), Bytes.toBytes(cf), Bytes.toBytes(e._2), System.currentTimeMillis(), Bytes.toBytes(e._3))

        (new ImmutableBytesWritable(Bytes.toBytes(e._1)), kv)
      })
    data
  }

  def hbaseBulkLoad(df: DataFrame, hbaseTableName: String)(implicit spark: SparkSession): Unit = {

    val tableName = TableName.valueOf(hbaseTableName)
    val conf: Configuration = HBaseConfiguration.create()

    val conn: Connection = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(tableName)

    val regionLocator = conn.getRegionLocator(tableName)

    val job = Job.getInstance(conf, "HFile Generator")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    // 3. 生成HFile
    val hResult: RDD[(ImmutableBytesWritable, KeyValue)] = transDataFrameToRDDHFile(df, "rowKeyField")
    val hFilePath = "hdfs://ns1/tmp/hbase/table"

    hResult.saveAsNewAPIHadoopFile(
      hFilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    // 4. bulk load to hbase
    val admin: HBaseAdmin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    val bulkLoader = new BulkLoadHFilesTool(conf)
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    table.close()
    conn.close()
  }
}
