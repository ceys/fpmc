package com.jd.bdp.fpmc.consume

import com.jd.bdp.fpmc.entity.origin.Label
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

case class HbaseReaderParams(sql: String, row2Label: Row => Label,
                             hbaseStorages: Map[String, HbaseStorage],
                              fSet: Set[String]) extends Serializable

class HbaseReader(hrp: HbaseReaderParams) extends BaseReader[HiveContext, RDD[Example]] {

  @transient
  override def makeExamples(sqlContext: HiveContext): RDD[Example] = sqlContext.sql(hrp.sql).map(hrp.row2Label).mapPartitions{ partitionOfRecords => {
    val storages = hrp.hbaseStorages.map { case (key: String, hbaseStorage: HbaseStorage) =>
      val hbaseTable = hbaseStorage.create
      val cache = new mutable.HashMap[Array[Byte], Features]()
      (key, (hbaseTable, cache))
    }

    partitionOfRecords.map { a: Label =>
      val fArray = a.ids.flatMap { case (key: String, idArray: Array[FeaturesID]) =>
        idArray.map { id =>
          var features: Features = null
          val cache = storages.get(key).get._2
          if (cache.contains(id.toHbaseKey)) {
            cache.get(id.toHbaseKey).get
          } else {
            if (hrp.fSet.isEmpty) {
              features = storages.get(key).get._1.pullFeatures(id)
            } else {
              features = storages.get(key).get._1.pullFeatures(id, hrp.fSet)
            }
          }
          features
        }
      }.toArray
      Example(a.label, fArray)
    }
  }
  }

}
