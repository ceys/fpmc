package com.jd.bdp.fpmc.o2o.jobs

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.Job
import com.jd.bdp.fpmc.entity.result.{Feature, Features, ItemFeaturesId}
import com.jd.bdp.fpmc.o2o.Constants
import com.jd.bdp.fpmc.produce.offline.{SparkSql2Hbase, SparkSql2HbaseParams}
import com.jd.bdp.fpmc.storage.HbaseStorage
import Constants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zhengchen on 2015/9/7.
 */
object ItemFeatureJob extends Job[SparkContext] {

  val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
  val DATE = (DateTime.now - 1.days).toString(fmt)
  val DATE_TIMESTAMP = (fmt.parseMillis(DATE) / 1000).toInt

  val item_sql = s"""
                select
                    id
                    ,nvl(categoryid, '')
                    ,nvl(brandid, '')
                    ,nvl(skuprice, -1)
                    ,nvl(skumarketprice, -1)
                    ,nvl(length, -1)
                    ,nvl(wide, -1)
                    ,nvl(high, -1)
                    ,nvl(weight, -1)
                    ,nvl(shopcategories, '')
                    ,nvl(orgcode, -1)
                from fdm.fdm_o2o_pms_sku_main_chain
                where
                start_date <= '$DATE' and end_date > '$DATE'
                and id != '' and id is not null and id != 'NULL'
                """

  def item_row_2_features(row: Row): Features = {
    val id = new ItemFeaturesId(row.getString(0).toLong, DATE_TIMESTAMP, true)
    val result = new Features(id)
    if (row.get(1) != null && row.get(1) != "" && row.get(1) != "NULL")
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_CATEGORYID, row.getString(1)))
    if (row.get(2) != null && row.get(2) != "" && row.get(2) != "NULL")
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_BRANDID, row.getString(2)))
    if (row.get(3) != null && row.get(3) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_SKUPRICE, row.getLong(3).toString))
    if (row.get(4) != null && row.get(4) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_SKUMARKETPRICE, row.getLong(4).toString))
    if (row.get(5) != null && row.get(5) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_LENGTH, row.getLong(5).toString))
    if (row.get(6) != null && row.get(6) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_WIDE, row.getLong(6).toString))
    if (row.get(7) != null && row.get(7) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_HIGH, row.getLong(7).toString))
    if (row.get(8) != null && row.get(8) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_WEIGHT, row.getLong(8).toString))
    if (row.get(9) != null && row.get(9) != "" && row.get(9) != "NULL")
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_SHOPCATEGORIES,
        row.getString(9).substring(1, row.getString(9).length-1)))
    if (row.get(10) != null && row.get(10) != -1)
      result.addFeature(new Feature(Constants.VW_NAME_ITEM_ORGCODE, row.getLong(10).toString))
    result
  }

  def run(sc: SparkContext) {
    val sqlContext = new HiveContext(sc)
    val hbaseStorage = new HbaseStorage( Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_ITEM_CROSS_TABLE
      , Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))

    val params = SparkSql2HbaseParams(item_sql, item_row_2_features,
      hbaseStorage)

    val workflow = new SparkSql2Hbase(params)
    val featureRdd = workflow.data2feature(sqlContext)
    workflow.feature2storage(featureRdd)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    run(sc)
    sc.stop()
  }

}