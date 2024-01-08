package com.example

import org.apache.spark.{SparkConf, SparkContext}
/**
 *
 * @author pengpeng.chen
 * @date 2021/4/14 7:14 下午
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountLocal")
    conf.setMaster("local")

    val sparkContext = new SparkContext(conf)
    val textFileRDD = sparkContext.textFile("src/main/resources/word.txt")
    val wordRDD = textFileRDD.flatMap(line => line.split(" "))
    val pairWordRDD = wordRDD.map(word => (word, 1))
    val wordCountRDD = pairWordRDD.reduceByKey((a, b) => a + b)

    wordCountRDD.saveAsTextFile("src/main/resources/wordcount")
  }
}
