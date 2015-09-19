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
