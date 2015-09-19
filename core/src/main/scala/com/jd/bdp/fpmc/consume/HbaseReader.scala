package com.jd.bdp.fpmc.consume

import com.jd.bdp.fpmc.entity.origin.Action
import com.jd.bdp.fpmc.entity.result.{FeaturesID, Features}
import com.jd.bdp.fpmc.storage.HbaseStorage
import main.scala.com.jd.bdp.fpmc.consume.BaseReader
import main.scala.com.jd.bdp.fpmc.entity.result.Example
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
 * Created by zhengchen on 2015/8/31.
 */

case class HbaseReaderParams(sql: String, row2Action: Row => Action,
                             hbaseStorages: Array[HbaseStorage]) extends Serializable

class HbaseReader(hrp: HbaseReaderParams) extends BaseReader[HiveContext, RDD[Example]] {

  @transient
  override def makeExamples(sqlContext: HiveContext): RDD[Example] = {
    sqlContext.sql(hrp.sql).map(hrp.row2Action).mapPartitions{ partitionOfRecords => {
      val storages = hrp.hbaseStorages.map { hbaseStorage =>
        val hbaseTable = hbaseStorage.create
        val cache = new mutable.HashMap[Array[Byte], Features]()
        (hbaseTable, cache)
      }

      partitionOfRecords.map { a =>
        val fsids: Array[FeaturesID] = a.getFsIds
        val fArray = fsids.map { fsid =>
          var features: Features = null
          if (cache.contains(fsid.toHbaseKey)){
            cache.get(fsid.toHbaseKey).get
          } else {
            features = hbaseTable.pullFeatures(fsid)
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
