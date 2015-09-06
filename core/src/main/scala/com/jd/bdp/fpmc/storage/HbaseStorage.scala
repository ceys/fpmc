package com.jd.bdp.fpmc.storage

import com.jd.bdp.fpmc.entity.result.{FeaturesID, Features, Feature}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
 * Created by zhengchen on 2015/8/27.
 */

class HbaseStorage(hbaseZK: String, parent: String,
                   tableName: String, family: Array[Byte]) extends Serializable {

  def create: HbaseTable = {
    new HbaseTable(hbaseZK, parent, tableName, family: Array[Byte])
  }

}

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
      for ((k,v) <- r.getNoVersionMap.get(family)) {
        result.addFeature(new Feature(Bytes.toString(k), Bytes.toString(v)))
      }
    }
    result
  }

}