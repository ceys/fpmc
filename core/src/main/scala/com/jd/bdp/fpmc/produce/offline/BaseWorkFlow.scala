package com.jd.bdp.fpmc.produce.offline

/**
 * Created by zhengchen on 2015/9/2.
 */
abstract class BaseWorkFlow[SC, FD, ST] extends Serializable {

  def data2feature(sc: SC): FD

  def feature2storage(fd: FD, st: ST)

}
