package com.chenpp.spark.spring

/**
 * @author April.Chen
 * @date 2023/4/28 3:41 下午
 * */
trait StatsTask extends Serializable {

  //"子类"继承StatsTask重写该方法实现自己的业务处理逻辑
  def runTask(etime: String)
}
