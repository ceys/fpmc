package com.jd.bdp.fpmc

import org.apache.spark.SparkContext
import scopt.OptionParser
import grizzled.slf4j.Logging

/**
 * Created by zhengchen on 2015/9/7.
 */
trait Job[C, J] {

  def run(context: C, jobContext: J)

}

abstract class SparkSqlJob extends Logging with Job[SparkContext, JobContext]{

  case class SparkSqlJobConfig(
                                date: String = "",
                                sql: String = ""
                                )

  val parser = new OptionParser[SparkSqlJobConfig]("FeatureMeta") {
    opt[String]("date") action {(g, c) =>
      c.copy(date = g)
    } text "shows all features meta data"
    opt[String]("sql") action {(g, c) =>
      c.copy(sql = g)
    } text "feature`s information json file to be pushed to meta data"
  }

  def alone(args: Array[String]): Unit = {

    val fmcOpt = parser.parse(args, SparkSqlJobConfig())
    if (fmcOpt.isEmpty) {
      logger.error("WorkflowConfig is empty. Quitting")
      return
    }
    val fmc = fmcOpt.get

  }

}
