package com.chenpp.spark.spring

import org.apache.spark.sql.SparkSession
import org.springframework.stereotype.Component

/**
 * @author April.Chen
 * @date 2023/4/28 3:42 下午
 * */
@Component
class WordCount extends StatsTask {

  override def runTask(etime: String): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val words = sparkSession.read.textFile("/Users/BigData/Documents/data/wordcount.txt").flatMap(_.split(" "))
      .toDF("word")

    words.createOrReplaceTempView("wordcount")

    val df = sparkSession.sql("select word, count(*) count from wordcount group by word")

    df.show()
  }
}
