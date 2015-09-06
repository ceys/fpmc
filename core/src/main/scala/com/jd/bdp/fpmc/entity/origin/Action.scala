package com.jd.bdp.fpmc.entity.origin

import com.jd.bdp.fpmc.entity.result.FeaturesID

import scala.collection.mutable.ListBuffer

/**
 * Created by zhengchen on 2015/8/25.
 */
class Action {

  private var label: Int = -1
  private var context: String = null
  private var fsIds: ListBuffer[FeaturesID] = new ListBuffer[FeaturesID]()

  def getLabel: Int = {
    label
  }

  def setLable(l: Int) {
    label = l
  }

  def getFsIds: Array[FeaturesID] = {
    fsIds.toArray
  }

  def addFsId(f: FeaturesID): Unit = {
    fsIds += f
  }

}
