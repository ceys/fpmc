package com.jd.bdp.fpmc

/**
 * Created by zhengchen on 2015/9/7.
 */
trait Job[C] {

  def run(context: C)

}
