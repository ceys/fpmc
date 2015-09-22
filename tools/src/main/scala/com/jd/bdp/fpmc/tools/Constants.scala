package com.jd.bdp.fpmc.tools

/**
 * Created by zhengchen on 2015/9/17.
 */
object Constants {

  val FTYPE_USER = "user"
  val FTYPE_ITEM = "item"
  val FTYPE_CROSS = "cross"

  //Hbase Feature Meta Rowkey
  val HBASE_FEATURE_META_ROWKEY = "hbase_feature_meta_rowkey"

  // hbase
  val HBASE_ZOOKEEPER_QUORUM = "BJHC-HBase-Magpie-17896.jd.local:2181," +
    "BJHC-HBase-Magpie-17895.jd.local:2181,BJHC-HBase-Magpie-17894.jd.local:2181," +
    "BJHC-HBase-Magpie-17893.jd.local:2181,BJHC-HBase-Magpie-17897.jd.local:2181"

  val HBASE_ZOOKEEPER_ZNODE_PARENT = "/hbase_han_river"

  // hbase family
  val HBASE_FEATURE_OFFLINE_FAMILY = "d"
  val HBASE_TABLE_DEFAULT_FAMILY = "d"



  // Hbase Meta data table
  val HBASE_META_DATA_TABLE = "fpmc_meta_data"

  // ================= hbase table ===============
  val HBASE_USER_ITEM_CROSS_TABLE = "o2o_user_item_feature"
  val HBASE_ITEM_FEATURE_TABLE = "o2o_item_feature"
  val HBASE_USER_FEATURE_TABLE = "o2o_user_feature"

  // ================= hbase table family ===============
  val HBASE_OFFLINE_FEATURE_FAMILY = "d"

}
