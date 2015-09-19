package com.jd.bdp.fpmc.o2o

import com.jd.bdp.fpmc.Job
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhengchen on 2015/9/14.
 */
object CreateFPMCWorkFlow {

  def run(): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val jobsClassNames = Array("com.jd.bdp.o2orec.feature.ItemBrandCrossFeatureJob",
      "com.jd.bdp.o2orec.feature.ItemFeatureJob",
      "com.jd.bdp.o2orec.label.HomePageBuyExampleJob")

    val jobs = jobsClassNames.map { className =>
      Class.forName(className).newInstance.asInstanceOf[Job[SparkContext]]
    }
    jobs.foreach(_.run(sc))

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run()
  }

}
