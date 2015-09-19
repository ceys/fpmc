package com.jd.bdp.fpmc.entity.result

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

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

  private val features = new ListBuffer[Feature]()
  private var id: FeaturesID = fid

  def addFeature(f: Feature): Features = {
    features += f
    this
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

/**
 * Item features id
 * @param sku
 * @param time
 * @param isOffline If true change the timestamp to be the 00:00 of the day.
 */
class ItemFeaturesId(sku: Long, time: Int, isOffline: Boolean) extends FeaturesID {

  override def toHbaseKey: Array[Byte] = {
    val key = ByteBuffer.allocate(4 + 8)
    if (isOffline) {
      key.putInt(time - new DateTime(time.toLong * 1000).getSecondOfDay)
    } else {
      key.putInt(time)
    }
    key.putLong(sku)
    key.array()
  }

}

class UserFeaturesId(user: String, time: Int, isOffline: Boolean) extends FeaturesID {

  override def toHbaseKey: Array[Byte] = {
    val key = ByteBuffer.allocate(4 + 8)
    if (isOffline) {
      key.putInt(time - new DateTime(time.toLong * 1000).getSecondOfDay)
    } else {
      key.putInt(time)
    }
    key.put(Bytes.toBytes(user))
    key.array()
  }
}


class CrossFeaturesId(uid: String, attrCate: Int, attrId: Long, time: Int, isOffline: Boolean) extends FeaturesID {

  override
  def toHbaseKey: Array[Byte] = {
    val key = ByteBuffer.allocate(4 + 4 + 8 + uid.size)
    if (isOffline) {
      key.putInt(time - new DateTime(time.toLong * 1000).getSecondOfDay)
    } else {
      key.putInt(time)
    }
    key.putInt(attrCate)
    key.putLong(attrId)
    key.put(uid.getBytes)
    key.array()
  }

}