package com.jd.bdp.fpmc.consume

import java.nio.ByteBuffer

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.storage.HbaseTable
import com.jd.bdp.fpmc.util.Constants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhengchen on 2015/8/31.
 */
object TestBatchRead {

  def main(args: Array[String]) {
    /*
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val DATE = (DateTime.now - 2.days).toString(fmt)

    val posLableSql =
      s"""
         select
             nvl(user_log_acct, ''),
             nvl(sku_id, -1),
             nvl(request_time_sec, -1),
             cast(1 as string)
         from gdm.gdm_m14_online_o2o
         where dt='$DATE' and ct_page in ('detail' ,'GoodsInfo')
         and sku_id is not null and refer_page in ('home','Home')
       """

    sqlContext.sql(posLableSql).rdd.mapPartitions { partitionOfRecords => {
      val hbaseTable = new HbaseTable(Constants.HBASE_ZOOKEEPER_QUORUM, Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_ITEM_FEATURE_TABLE)
      partitionOfRecords.map { case Row(user_log_acct: String,
          sku_id: Long,
          request_time_sec: Int,
          label: String) =>
        if (sku_id != -1 && request_time_sec != -1) {
          val itemKey = ByteBuffer.allocate(2 * 8)
          val dateTime = request_time_sec - new DateTime(request_time_sec.toLong * 1000).getSecondOfDay
          itemKey.putLong(dateTime.toLong)
          itemKey.putLong(sku_id)
          val features = hbaseTable.pullFeatures(itemKey.array(), Bytes.toBytes(Constants.HBASE_FEATURE_OFFLINE_FAMILY))
          (label, features.mkString(" "))
        }
      }
    }
    }.saveAsTextFile("/tmp/zc/fpmc/test3")
    */
  }
}