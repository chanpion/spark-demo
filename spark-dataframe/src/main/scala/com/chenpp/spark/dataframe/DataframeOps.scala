package com.chenpp.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author April.Chen
 * @date 2023/9/6 3:24 下午
 * */
class DataframeOps {

  def ops(df: DataFrame)(implicit spark: SparkSession): Unit = {
    df.show(10, false)
    df.first()
    df.head(1)
    val count: Long = df.count()
    //cache()同步数据的内存
    df.cache()
    //返回一个string类型的数组，返回值是所有列的名字
    val columns = df.columns
  }
}
