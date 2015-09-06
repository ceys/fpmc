package com.jd.bdp.fpmc.produce.offline

import com.jd.bdp.fpmc.entity.result.Feature
import com.jd.bdp.fpmc.storage.HbaseTable
import com.jd.bdp.fpmc.util.Constants

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

import java.nio.ByteBuffer
import scala.collection.mutable


/**
 * Created by zhengchen on 2015/8/26.
 */
object TestHive {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val date = (DateTime.now - 1.days).toString(fmt)
    val dateTime = fmt.parseMillis(date) / 1000
    println(date)
    println(dateTime)

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
                              ,nvl(weight, 0)-1
                              ,nvl(shopcategories,'')
                              ,nvl(stocknum, -1)
                              ,nvl(orgcode, -1)
                          from fdm.fdm_o2o_pms_sku_main_chain
                          where
                              start_date <= '$date' and end_date > '$date'
                              and id != ''
                """

    //1016475960,4698,32220,10000,109900,1,1,1,1,[470609 470610 470608 470621 847483 470594 470623 1883925 1723745 1883967],4878,71550
    sqlContext.sql(item_sql).rdd.mapPartitions { partitionOfRecords => {
      val hbaseTable = new HbaseTable(Constants.HBASE_ZOOKEEPER_QUORUM, Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_ITEM_FEATURE_TABLE)
      partitionOfRecords.map { case Row(id: String
                                      , categoryid: String
                                      , brandid: String
                                      , skuprice: Long
                                      , skumarketprice: Long
                                      , length: Long
                                      , wide: Long
                                      , high: Long
                                      , weight: Long
                                      , shopcategories: String
                                      , stocknum: Long
                                      , orgcode: Long) =>
          val features = new mutable.ListBuffer[Feature]
          if (categoryid != "") {
            features += new Feature(Constants.VW_NAME_ITEM_CATEGORYID, categoryid)
          }
          if (brandid != "") {
            features += new Feature(Constants.VW_NAME_ITEM_BRANDID, brandid)
          }
          if (skuprice != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_SKUPRICE, skuprice.toString)
          }
          if (skumarketprice != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_SKUMARKETPRICE, skumarketprice.toString)
          }
          if (length != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_LENGTH, length.toString)
          }
          if (wide != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_WIDE, wide.toString)
          }
          if (high != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_HIGH, high.toString)
          }
          if (weight != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_WEIGHT, weight.toString)
          }
          if (shopcategories != "") {
            features += new Feature(Constants.VW_NAME_ITEM_SHOPCATEGORIES, shopcategories.substring(1, shopcategories.length-1))
          }
          if (stocknum != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_STOCKNUM, stocknum.toString)
          }
          if (orgcode != -1) {
            features += new Feature(Constants.VW_NAME_ITEM_ORGCODE, orgcode.toString)
          }
          val key = ByteBuffer.allocate(2 * 8)
          key.putLong(dateTime)
          key.putLong(id.toLong)
          //hbaseTable.pushFeatures(key.array(), Bytes.toBytes(Constants.HBASE_FEATURE_OFFLINE_FAMILY), features.toArray)
          (key.array(), features.mkString(" "))
        }
      }
    }.mapPartitions { partitionOfRecords => {
        val hbaseTable = new HbaseTable(Constants.HBASE_ZOOKEEPER_QUORUM, Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_ITEM_FEATURE_TABLE)
        partitionOfRecords.map { case (k: Array[Byte], v: String) =>
          (k, hbaseTable.pullFeatures(k, Bytes.toBytes(Constants.HBASE_FEATURE_OFFLINE_FAMILY)).mkString(" "))
        }
      }
    }.saveAsTextFile("/tmp/zc/fpmc/test2")

    sc.stop()

  }

}
