package com.chenpp.spark.dataframe

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author April.Chen
 * @date 2023/9/6 3:17 下午
 * */
class DataframeBuilder {

  def createDataframe()(implicit spark: SparkSession): DataFrame = {
    val schema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    val df = spark.createDataFrame(Seq(("ming", 20), ("Michael", 30), ("Andy", 18))).toDF("name", "age")
    df

  }

  def readFromJson(path: String)(implicit spark: SparkSession): DataFrame = {
    //spark.read.json("file:///opt/spark/testfile/people.json")
    spark.read.json(path)
  }

  def readFromCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.option("seq", ",").option("header", "true").option("multiLine", "true").option("encoding", "utf-8")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("inferSchema", value = true).csv(path)
  }
}
