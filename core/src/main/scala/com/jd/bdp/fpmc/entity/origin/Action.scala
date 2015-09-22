package com.jd.bdp.fpmc.entity.origin

import com.jd.bdp.fpmc.entity.result.FeaturesID

import scala.collection.mutable.ListBuffer

/**
 * Created by zhengchen on 2015/8/25.
 */
case class Label(label: Int, context: String, ids: Map[String, Array[FeaturesID]])