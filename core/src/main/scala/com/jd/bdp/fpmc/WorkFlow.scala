package com.jd.bdp.fpmc

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhengchen on 2015/9/8.
 */
class WorkFlow[C](jobClassNames: Array[String]) {

  def start(context: C) = {
    val jobs = jobClassNames.map { className =>
      Class.forName(className).newInstance.asInstanceOf[Job[C]]
    }
    jobs.foreach(_.run(context))
  }

}

object CreateFPMCWorkFlow {

  def run(): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val jobs = Array("com.jd.bdp.o2orec.feature.ItemBrandCrossFeatureJob")

    sc.stop()
  }

  def main(args: Array[String]): Unit = {


  }

}
