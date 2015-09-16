package com.jd.bdp.fpmc.entity.origin

/**
 * Used by Meta data.
 *
 * Created by zhengchen on 2015/9/14.
 */
case class FeatureDescription(vwName: String, description: String,
                              user: String, date: String) extends Serializable {

  def toDescription: String = {
    s"$vwName\001$description\001$user\001$date"
  }

}

object FeatureDescription {

  def fromDescrition(dsp: String): FeatureDescription = {
    val rows: Array[String] = dsp.split("\001")
    FeatureDescription(rows(0), rows(1), rows(2), rows(3))
  }

}
