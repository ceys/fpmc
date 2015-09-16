package com.jd.bdp.fpmc

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

/**
 * Created by zhengchen on 2015/9/8.
 */
/*class WorkFlow[C](jobClassNames: Array[String]) {

  def start(context: C) = {
    val jobs = jobClassNames.map { className =>
      Class.forName(className).newInstance.asInstanceOf[Job[C]]
    }
    jobs.foreach(_.run(context))
  }

}*/

case class JobContext(date: DateTime) {

}


