package com.jd.bdp.fpmc.consume

import com.jd.bdp.fpmc.entity.origin.Action
import com.jd.bdp.fpmc.entity.result.{FeaturesID, Features}
import com.jd.bdp.fpmc.storage.HbaseStorage
import main.scala.com.jd.bdp.fpmc.entity.result.Example
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by zhengchen on 2015/8/31.
 */

case class HbaseReaderParams(fName: Array[String], hbaseStorage: HbaseStorage,
                              hbaseTableFamily: Array[Byte]) extends Serializable

class HbaseReader(hrp: HbaseReaderParams) {

  def combineLabelFeatures(action: RDD[Action]): RDD[Example] = {
    action.mapPartitions{ partitionOfRecords => {
      val hbaseTable = hrp.hbaseStorage.create
      val cache = new mutable.HashMap[Array[Byte], Features]()
      partitionOfRecords.map { a =>
        val fsids: Array[FeaturesID] = a.getFsIds
        val fArray = fsids.map { fsid =>
          var features: Features = null
          if (cache.contains(fsid.toHbaseKey)){
            cache.get(fsid.toHbaseKey).get
          } else {
            features = hbaseTable.pullFeatures(fsid, hrp.hbaseTableFamily)
            cache.put(fsid.toHbaseKey, features)
          }
          features
        }
        Example(a.getLabel, fArray)
      }
    }
    }
  }


}