package com.chenpp.spark.spring

import org.slf4j.LoggerFactory

/**
 * @author April.Chen
 * @date 2023/4/28 3:39 下午
 * */
object Bootstrap {
  private val log = LoggerFactory.getLogger(Bootstrap.getClass)

  //指定配置文件如log4j的路径
  val ConfFileName = "conf"
  val ConfigurePath = new File("").getAbsolutePath.substring(0, if (new File("").getAbsolutePath.lastIndexOf("lib") == -1) 0
  else new File("").getAbsolutePath.lastIndexOf("lib")) + this.ConfFileName + File.separator

  //存放实现了StatsTask的离线程序处理的类
  private val TASK_MAP = Map("WordCount" -> classOf[WordCount])

  def main(args: Array[String]): Unit = {
    //传入一些参数，比如要运行的离线处理程序类名、处理哪些时间的数据
    if (args.length < 1) {
      log.warn("args 参数异常！！！" + args.toBuffer)
      System.exit(1)
    }
    init(args)
  }

  def init(args: Array[String]) {
    try {
      SpringUtils.init(Array[String]("applicationContext.xml"))
      initLog4j()

      val className = args(0)
      // 实例化离线处理类
      val task = SpringUtils.getBean(TASK_MAP(className))

      args.length match {
        case 3 =>
          // 处理一段时间的每天离线数据
          val dtStart = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(args(1))
          val dtEnd = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(args(2))
          val days = Days.daysBetween(dtStart, dtEnd).getDays + 1
          for (i <- 0 until days) {
            val etime = dtStart.plusDays(i).toString("yyyy-MM-dd")
            task.runTask(etime)

            log.info(s"JOB --> $className 已成功处理: $etime 的数据")
          }

        case 2 =>
          // 处理指定的某天离线数据
          val etime = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(args(1)).toString("yyyy-MM-dd")
          task.runTask(etime)
          log.info(s"JOB --> $className 已成功处理: $etime 的数据")

        case 1 =>
          // 处理前一天离线数据
          val etime = DateTime.now().minusDays(1).toString("yyyy-MM-dd")
          task.runTask(etime)
          log.info(s"JOB --> $className 已成功处理: $etime 的数据")

        case _ => println("执行失败 args参数:" + args.toBuffer)
      }
    } catch {
      case e: Exception =>
        println("执行失败 args参数:" + args.toBuffer)
        e.printStackTrace()
    }

    // 初始化log4j
    def initLog4j() {
      val fileName = ConfigurePath + "log4j.properties"
      if (new File(fileName).exists) {
        PropertyConfigurator.configure(fileName)
        log.info("日志log4j已经启动")
      }
    }
  }
}