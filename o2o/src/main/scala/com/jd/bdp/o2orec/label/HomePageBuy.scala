package com.jd.bdp.o2orec.label

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.consume.{HbaseReader, HbaseReaderParams}
import com.jd.bdp.fpmc.entity.origin.Action
import com.jd.bdp.fpmc.entity.result.CrossFeaturesId
import com.jd.bdp.fpmc.storage.HbaseStorage
import com.jd.bdp.o2orec.Constants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zhengchen on 2015/9/6.
 */
object HomePageBuy {

  def click2Action(row: Row): Action = {
    val result = new Action()
    result.setLable(row.getInt(3))
    val cfid = new CrossFeaturesId(row.getString(0), Constants.CROSS_FEATURES_BRAND_ID, row.getInt(1), row.getInt(2))
    result.addFsId(cfid)
    result
  }

  def run(): Unit = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val DATE = (DateTime.now - 1.days).toString(fmt)
    //val YESTERDAY_TIMESTAMP = (fmt.parseMillis(YESTERDAY) / 1000).toInt
    val clickSql =
      s"""
         |select
         |    user_log_acct,
         |    sku_id,
         |    request_time_sec,
         |    1
         |from gdm.gdm_m14_online_o2o
         |where dt='$DATE' and ct_page in ('detail' ,'GoodsInfo')
                                  |and sku_id is not null and refer_page in ('home','Home')
      """.stripMargin

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val hbaseStorage = new HbaseStorage(Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_ITEM_CROSS_TABLE,
      Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))

    val readerParams = new HbaseReaderParams(clickSql, click2Action, hbaseStorage)
    val reader = new HbaseReader(readerParams)

    val exampleRDD = reader.makeExamples(sqlContext)

    sc.stop()
  }

}
