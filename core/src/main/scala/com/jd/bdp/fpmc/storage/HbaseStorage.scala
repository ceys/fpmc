package com.jd.bdp.fpmc.storage

import com.jd.bdp.fpmc.FeatureNameAlreadyUsedException
import com.jd.bdp.fpmc.entity.origin.FeatureDescription
import com.jd.bdp.fpmc.entity.result.{FeaturesID, Features, Feature}
import com.jd.bdp.fpmc.util.Constants

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Because Spark need to serialize the object to worker while
 * HTable is not serializable, so we serialize HbaseStorage
 * and create hbase connection on each partition.
 *
 * Created by zhengchen on 2015/8/27.
 */

class HbaseStorage(hbaseZK: String, parent: String,
                   tableName: String, family: Array[Byte]) extends Serializable {

  def create: HbaseTable = {
    new HbaseTable(hbaseZK, parent, tableName, family: Array[Byte])
  }

}


//TODO(zhengchen): Write different traits that define different functions, and implement them separately.
class HbaseTable(hbaseZK: String, parent: String,
                 tableName: String, family: Array[Byte]) {

  private val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum", hbaseZK)
  hbaseConf.set("zookeeper.znode.parent", parent)

  private val table = new HTable(hbaseConf, tableName)


  def pushFeatures(fsid: FeaturesID, features: Features) {
    val p = new Put(fsid.toHbaseKey)
    features.foreach(f => p.add(family, Bytes.toBytes(f.name), Bytes.toBytes(f.value)))
    table.put(p)
  }


  def pullFeatures(fsid: FeaturesID): Features = {
    val get: Get = new Get(fsid.toHbaseKey).addFamily(family)
    val r = table.get(get)
    val result = new Features(fsid)
    if (r != null) {
      val featureMap = r.getNoVersionMap
      if (featureMap != null) {
        for ((k,v) <- featureMap.get(family)) {
          result.addFeature(new Feature(Bytes.toString(k), Bytes.toString(v)))
        }
      }
    }
    result
  }

  /**
   * Push a new vw namespace into the meta data.
   * If the namespace is already exist, then throws teh FeatureNameAlreadyUsedException.
   * @param dsp FeatureDescription
   */
  def pushFeatureMeta(dsp: FeatureDescription): Unit = {
    if (featureNameIsExist(dsp.vwName)) {
      throw FeatureNameAlreadyUsedException(dsp.vwName)
    } else {
      val p = new Put(Bytes.toBytes(Constants.HBASE_FEATURE_META_ROWKEY))
      p.add(family, Bytes.toBytes(dsp.vwName),
        Bytes.toBytes(dsp.toDescription))
      table.put(p)
    }
  }

  /**
   * Get all feature information as a Map which key is vw namespace
   * and value is a description of the feature.
   * @return
   */
  def getAllFeatureMeta: Map[String, FeatureDescription] = {
    val result = new mutable.HashMap[String, FeatureDescription]()
    val get = new Get(Bytes.toBytes(Constants.HBASE_FEATURE_META_ROWKEY)).addFamily(family)
    val r = table.get(get)
    if (r != null) {
      val metaMap = r.getNoVersionMap
      if (metaMap != null) {
        for ((k, v) <- metaMap.get(family)) {
          result.put(Bytes.toString(k), FeatureDescription.fromDescrition(Bytes.toString(v)))
        }
      }
    }
    result.toMap
  }

  /**
   * Check the vw feature namespace whether in the meta data or not.
   * @param id vw namespace
   * @return
   */
  def featureNameIsExist(id: String): Boolean = {
    var result = false
    val get = new Get(Bytes.toBytes(Constants.HBASE_FEATURE_META_ROWKEY)).addFamily(family)
    val r = table.get(get)
    if (r != null) {
      result = r.containsColumn(family, Bytes.toBytes(id))
    }
    result
  }

}