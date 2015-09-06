package com.jd.bdp.fpmc.entity.result

import java.nio.ByteBuffer

import scala.collection.mutable

/**
 * Created by zhengchen on 2015/8/25.
 */
class Feature(n: String, v: String) extends Serializable {

  val name = n
  val value = v

  override def toString: String = {
    s"| $name $value "
  }

  def toVW: String = {
    s"| $name $value "
  }

}


class Features(fid: FeaturesID) extends Iterable[Feature] with Serializable {

  private val features = new mutable.LinkedList[Feature]
  private var id: FeaturesID = fid

  def addFeature(f: Feature): Unit = {
    features.+:(f)
  }

  def toVW: String = {
    features.map(_.toVW).mkString(" ")
  }

  def setId(featureId: FeaturesID) {
    id = featureId
  }

  def getId: FeaturesID = id

  override
  def iterator = features.iterator

}


abstract class FeaturesID extends Serializable {

  def toHbaseKey: Array[Byte]

}


class CrossFeaturesId(uid: String, attrCate: Char, attrId: Int, time: Int) extends FeaturesID {

  override
  def toHbaseKey: Array[Byte] = {
    val key = ByteBuffer.allocate(4 + 1 + 4 + uid.size)
    key.putInt(time)
    key.putChar(attrCate)
    key.putInt(attrId)
    key.put(uid.getBytes)
    key.array()
  }

}